package main

import (
	"embed"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"strconv"
	"text/template"

	"golang.org/x/tools/go/packages"
)

//go:embed generator/templates/*.tpl
var templates embed.FS

type Loader struct {
	errorType    types.Type
	contextType  types.Type
	grainRefType *types.Interface
	protoMessage *types.Interface
	goorGenPkg   *packages.Package
	pkg          *packages.Package
	fset         *token.FileSet
}

func NewLoader(path string) (*Loader, error) {
	fset := token.NewFileSet()
	pkgs, err := packages.Load(&packages.Config{
		Fset: fset,
		Mode: packages.NeedSyntax | packages.NeedName | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports,
	}, path, "github.com/jaym/go-orleans/goor-gen/goor")

	if err != nil {
		return nil, err
	}

	var goorGenPkg *packages.Package
	var grainRefType *types.Interface
	var protoMessage *types.Interface
	var contextType types.Type
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		switch p.PkgPath {
		case "github.com/jaym/go-orleans/goor-gen/goor":
			goorGenPkg = p
		case "google.golang.org/protobuf/reflect/protoreflect":
			s := p.Types.Scope()
			obj := s.Lookup("ProtoMessage")
			if obj == nil {
				panic("couldn't find proto.Message")
			}
			protoMessage = obj.Type().Underlying().(*types.Interface)
		case "github.com/jaym/go-orleans/grain":
			s := p.Types.Scope()
			obj := s.Lookup("GrainReference")
			if obj == nil {
				panic("could not find github.com/jaym/go-orleans/grain/goor.GrainReference")
			}
			grainRefType = obj.Type().Underlying().(*types.Interface)
		case "context":
			s := p.Types.Scope()
			obj := s.Lookup("Context")
			if obj == nil {
				panic("could not find context.Context")
			}
			contextType = obj.Type()
		}
	})

	if len(pkgs) != 2 {
		return nil, errors.New("expected 2 packages")
	}
	userPkg := pkgs[0]
	for i := 0; i < len(pkgs); i++ {
		if pkgs[i].PkgPath == "github.com/jaym/go-orleans/goor-gen/goor" {
			continue
		}
		userPkg = pkgs[i]
	}

	return &Loader{
		errorType:    types.Universe.Lookup("error").Type(),
		goorGenPkg:   goorGenPkg,
		grainRefType: grainRefType,
		protoMessage: protoMessage,
		contextType:  contextType,
		pkg:          userPkg,
		fset:         fset,
	}, nil
}

func (l *Loader) listInterfaces(syntax []*ast.File, filter func(*ast.InterfaceType) bool) []*ast.TypeSpec {
	ifaces := []*ast.TypeSpec{}

	for _, astFile := range syntax {
		ast.Inspect(astFile, func(n ast.Node) bool {
			if typeSpec, ok := n.(*ast.TypeSpec); ok {
				if ifaceTy, ok := typeSpec.Type.(*ast.InterfaceType); ok {
					if filter(ifaceTy) {
						ifaces = append(ifaces, typeSpec)
					}
				}
			}
			return true
		})
	}

	return ifaces
}

func (l *Loader) findGoorInterfaces(pkg *packages.Package, implementsType string) []*ast.TypeSpec {
	return l.listInterfaces(pkg.Syntax, func(ifaceTy *ast.InterfaceType) bool {
		for _, f := range ifaceTy.Methods.List {
			tv, ok := pkg.TypesInfo.Types[f.Type]
			if !ok {
				panic("could not look up type info")
			}
			if named, ok := tv.Type.(*types.Named); ok {
				tn := named.Obj()
				if !tn.Exported() {
					continue
				}
				if tn.Pkg() == l.goorGenPkg.Types {
					if tn.Name() == implementsType {
						return true
					}
				}
			}
		}
		return false
	})
}

func (l *Loader) isGrainInterface(named *types.Named) bool {
	tn := named.Obj()
	if !tn.Exported() {
		return false
	}
	if tn.Pkg() == l.goorGenPkg.Types {
		if tn.Name() == "Grain" || tn.Name() == "ObservableGrain" {
			return true
		}
	}
	return false
}

func (l *Loader) isObserverInterface(named *types.Named) bool {
	tn := named.Obj()
	if !tn.Exported() {
		return false
	}
	if tn.Pkg() == l.goorGenPkg.Types {
		if tn.Name() == "Observer" {
			return true
		}
	}
	return false
}

func (l *Loader) isObserver(named *types.Named) bool {
	tn := named.Obj()
	if !tn.Exported() {
		return false
	}

	iface, ok := named.Underlying().(*types.Interface)
	if !ok {
		return false
	}

	for i := 0; i < iface.NumEmbeddeds(); i++ {
		e := iface.EmbeddedType(i)
		enamed, ok := e.(*types.Named)
		if !ok {
			continue
		}
		if enamed.Obj().Pkg() == l.goorGenPkg.Types && enamed.Obj().Name() == "Observer" {
			return true
		}
	}
	return false
}
func (l *Loader) isObserverRegistrationToken(named *types.Named) bool {
	obj := named.Obj()
	if obj.Pkg() == nil {
		return false
	}
	return obj.Pkg().Path() == "github.com/jaym/go-orleans/grain" && obj.Name() == "ObserverRegistrationToken"
}

func (l *Loader) isError(typ types.Type) bool {
	return typ == l.errorType
}

func (l *Loader) isContext(typ types.Type) bool {
	return typ == l.contextType
}

func (l *Loader) createParameter(v *types.Var, position int) *GoorParameter {
	_, isPointer := v.Type().(*types.Pointer)
	bt, isBasic := v.Type().Underlying().(*types.Basic)
	serializerType := "Interface"
	var basicTypeName string
	var basicSerializeTypeName string
	if isBasic {
		basicTypeName = bt.Name()
		basicSerializeTypeName = basicTypeName
		btInfo := bt.Info()
		if btInfo&types.IsBoolean != 0 {
			serializerType = "Bool"
		} else if btInfo&types.IsInteger != 0 {
			if btInfo&types.IsUnsigned != 0 {
				serializerType = "UInt64"
				basicSerializeTypeName = "uint64"
			} else {
				serializerType = "Int64"
				basicSerializeTypeName = "int64"
			}
		} else if btInfo&types.IsFloat != 0 {
			serializerType = "Float"
			basicSerializeTypeName = "float64"
		} else if btInfo&types.IsString != 0 {
			serializerType = "String"
		}
	}
	named, isNamedType := v.Type().(*types.Named)
	isObserver := false
	if isNamedType {
		isObserver = l.isObserver(named)
		if isObserver {
			serializerType = "Text"
		}
		if l.isObserverRegistrationToken(named) {
			serializerType = "Text"
		}
	}
	name := v.Name()
	if v.Name() == "" {
		name = "p" + strconv.Itoa(position)
	}
	return &GoorParameter{
		Name:                   name,
		Type:                   v.Type(),
		SerializerType:         serializerType,
		BasicTypeName:          basicTypeName,
		BasicSerializeTypeName: basicSerializeTypeName,
		IsPointer:              isPointer,
		IsBasic:                isBasic,
		IsObserver:             isObserver,
		l:                      l,
	}
}

func (l *Loader) createMethods(pkg *packages.Package, ifaceTy *ast.InterfaceType) ([]*GoorMethod, error) {
	methods := []*GoorMethod{}
	for _, f := range ifaceTy.Methods.List {
		tv, ok := pkg.TypesInfo.Types[f.Type]
		if !ok {
			return nil, errors.New("could not look up type info for field")
		}
		if m, ok := tv.Type.(*types.Signature); ok {
			method := &GoorMethod{
				l:    l,
				Doc:  f.Doc.Text(),
				Name: f.Names[0].Name,
			}
			for i := 0; i < m.Params().Len(); i++ {
				p := l.createParameter(m.Params().At(i), i)
				method.Parameters = append(method.Parameters, p)
			}
			for i := 0; i < m.Results().Len(); i++ {
				p := l.createParameter(m.Results().At(i), i)
				method.Returns = append(method.Returns, p)
			}
			methods = append(methods, method)
		} else if named, ok := tv.Type.(*types.Named); ok {
			if !(l.isGrainInterface(named) || l.isObserverInterface(named)) {
				return nil, errors.New("embedded interfaces not supported")
			}
		}
	}
	return methods, nil
}

type GoorParameter struct {
	Name                   string
	Type                   types.Type
	SerializerType         string
	BasicTypeName          string
	BasicSerializeTypeName string
	IsPointer              bool
	IsBasic                bool
	IsObserver             bool

	l *Loader
}

func (p *GoorParameter) IsGrainReference() bool {
	return types.Implements(p.Type.Underlying(), p.l.grainRefType)
}

func (p *GoorParameter) IsProtoMessage() bool {
	return types.Implements(p.Type.Underlying(), p.l.protoMessage)
}

func (p *GoorParameter) IsError() bool {
	return p.l.isError(p.Type)
}

func (p *GoorParameter) IsContext() bool {
	return p.l.isContext(p.Type)
}

func (p *GoorParameter) validate() error {
	return nil
}

type GoorMethod struct {
	Name       string
	Doc        string
	Parameters []*GoorParameter
	Returns    []*GoorParameter

	l *Loader
}

func (m *GoorMethod) validate() error {
	if m.Name == "" {
		panic("missing method name")
	}

	if len(m.Parameters) < 1 {
		return errors.New("grain methods must receive at least 1 parameter (context.Context)")
	}

	if !m.Parameters[0].IsContext() {
		return errors.New("grain method's first parameter must be context.Context")
	}

	for _, gp := range m.Parameters {
		if err := gp.validate(); err != nil {
			return err
		}
	}

	if len(m.Returns) > 0 {
		if !m.Returns[len(m.Returns)-1].IsError() {
			return errors.New("grain method's last parameter must be error")
		}
	}
	return nil
}

func (m *GoorMethod) IsOneWay() bool {
	return len(m.Returns) == 0
}

type GoorGrainDefinition struct {
	Name         string
	Methods      []*GoorMethod
	IsObservable bool
}

func (def *GoorGrainDefinition) validate() error {
	if def.Name == "" {
		panic("missing grain name")
	}

	for _, gm := range def.Methods {
		if err := gm.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (def *GoorGrainDefinition) GetName() string {
	return def.Name
}

type GoorObserverDefinition struct {
	Name    string
	Methods []*GoorMethod
}

func (def *GoorObserverDefinition) GetName() string {
	return def.Name
}

func (l *Loader) createGrainDef(pkg *packages.Package, gi *ast.TypeSpec, observableGrainDef *GoorGrainDefinition, isObservable bool) (*GoorGrainDefinition, error) {
	methods, err := l.createMethods(pkg, gi.Type.(*ast.InterfaceType))
	if err != nil {
		return nil, err
	}

	gd := &GoorGrainDefinition{
		Name:    gi.Name.Name,
		Methods: methods,
	}

	if isObservable {
		gd.Methods = append(gd.Methods, observableGrainDef.Methods...)
	}

	if err := gd.validate(); err != nil {
		return nil, err
	}

	return gd, nil
}

func (l *Loader) createObserverDef(pkg *packages.Package, gi *ast.TypeSpec) (*GoorObserverDefinition, error) {
	methods, err := l.createMethods(pkg, gi.Type.(*ast.InterfaceType))
	if err != nil {
		return nil, err
	}

	for _, gm := range methods {
		if !gm.IsOneWay() {
			return nil, errors.New("observer interfaces only support one way methods")
		}
	}

	od := &GoorObserverDefinition{
		Name:    gi.Name.Name,
		Methods: methods,
	}

	return od, nil
}

func (l *Loader) Load() ([]*GoorGrainDefinition, []*GoorObserverDefinition, error) {
	grainDefs := []*GoorGrainDefinition{}

	grainInterfaces := l.findGoorInterfaces(l.pkg, "Grain")
	for _, gi := range grainInterfaces {
		gd, err := l.createGrainDef(l.pkg, gi, nil, false)
		if err != nil {
			return nil, nil, err
		}

		grainDefs = append(grainDefs, gd)
	}

	observableGrains := l.findGoorInterfaces(l.goorGenPkg, "Grain")
	if len(observableGrains) != 1 {
		panic(len(observableGrains))
	}
	observableGrain := observableGrains[0]
	observableGrainDef, err := l.createGrainDef(l.goorGenPkg, observableGrain, nil, false)
	if err != nil {
		return nil, nil, err
	}

	grainInterfaces = l.findGoorInterfaces(l.pkg, "ObservableGrain")
	for _, gi := range grainInterfaces {
		gd, err := l.createGrainDef(l.pkg, gi, observableGrainDef, true)
		if err != nil {
			return nil, nil, err
		}

		grainDefs = append(grainDefs, gd)
	}

	observerDefs := []*GoorObserverDefinition{}
	observerInterfaces := l.findGoorInterfaces(l.pkg, "Observer")
	for _, oi := range observerInterfaces {
		od, err := l.createObserverDef(l.pkg, oi)
		if err != nil {
			return nil, nil, err
		}
		observerDefs = append(observerDefs, od)
	}

	return grainDefs, observerDefs, nil
}

type GoorDefNamed interface {
	GetName() string
}

func main() {
	l, err := NewLoader(os.Args[1])
	if err != nil {
		panic(err)
	}
	grainDefs, observerDefs, err := l.Load()
	if err != nil {
		panic(err)
	}

	// "github.com/jaym/go-orleans-chat-example/gen"
	// __grain "github.com/jaym/go-orleans/grain"
	// __generic "github.com/jaym/go-orleans/grain/generic"
	// __descriptor "github.com/jaym/go-orleans/grain/descriptor"
	// "github.com/jaym/go-orleans/grain/services"

	imports := map[string]GoorImports{
		"github.com/jaym/go-orleans/grain": {
			PackagePath: "github.com/jaym/go-orleans/grain",
			Name:        "__grain",
		},
		"github.com/jaym/go-orleans/grain/generic": {
			PackagePath: "github.com/jaym/go-orleans/grain/generic",
			Name:        "__generic",
		},
		"github.com/jaym/go-orleans/grain/descriptor": {
			PackagePath: "github.com/jaym/go-orleans/grain/descriptor",
			Name:        "__descriptor",
		},
	}

	outputPkgPath := l.pkg.PkgPath
	t, err := template.New("").Funcs(template.FuncMap{
		"qualifiedGrainType": func(gd GoorDefNamed) string {
			return gd.GetName()
		},
		"qualifiedArgType": func(p *GoorParameter) string {
			t := p.Type
			if pt, isPointer := t.(*types.Pointer); isPointer {
				t = pt.Elem()
			}
			switch tt := t.(type) {
			case *types.Basic:
				return tt.Name()
			case *types.Named:
				if outputPkgPath == tt.Obj().Pkg().Path() {
					return tt.Obj().Name()
				} else {
					if i, ok := imports[tt.Obj().Pkg().Path()]; ok {
						return i.Name + "." + tt.Obj().Name()

					}
					return tt.Obj().Pkg().Name() + "." + tt.Obj().Name()
				}
			}
			panic("cannot handle type")
		},
		"isOneWay": func(gm *GoorMethod) bool {
			return gm.IsOneWay()
		},
		"stripLastParam": func(params []*GoorParameter) []*GoorParameter {
			return params[0 : len(params)-1]
		},
	}).ParseFS(templates, "generator/templates/*")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stdout, header)
	for _, grainDef := range grainDefs {
		err = t.ExecuteTemplate(os.Stdout, "Grain", grainDef)
		if err != nil {
			panic(err)
		}
	}
	for _, od := range observerDefs {
		err = t.ExecuteTemplate(os.Stdout, "Observer", od)
		if err != nil {
			panic(err)
		}
	}
}

type GoorImports struct {
	PackagePath string
	Name        string
}

var header = `package gengo

import (
	"context"
	"errors"

	"github.com/jaym/go-orleans-chat-example/gen"
	__grain "github.com/jaym/go-orleans/grain"
	__generic "github.com/jaym/go-orleans/grain/generic"
	__descriptor "github.com/jaym/go-orleans/grain/descriptor"
	__services "github.com/jaym/go-orleans/grain/services"
)
`
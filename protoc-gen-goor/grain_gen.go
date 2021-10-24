package main

import (
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
)

const (
	contextPackage       = protogen.GoImportPath("context")
	siloPackage          = protogen.GoImportPath("github.com/jaym/go-orleans/silo")
	grainPackage         = protogen.GoImportPath("github.com/jaym/go-orleans/grain")
	grainServicesPackage = protogen.GoImportPath("github.com/jaym/go-orleans/grain/services")
	descriptorPackage    = protogen.GoImportPath("github.com/jaym/go-orleans/grain/descriptor")
)

var (
	contextType              = contextPackage.Ident("Context")
	siloClientType           = grainPackage.Ident("SiloClient")
	grainRefType             = grainPackage.Ident("GrainReference")
	identityType             = grainPackage.Ident("Identity")
	registeredObserverType   = grainPackage.Ident("RegisteredObserver")
	grainObserverManagerType = grainServicesPackage.Ident("GrainObserverManager")
	coreGrainServicesType    = grainServicesPackage.Ident("CoreGrainServices")
)

func GenerateGrain(g *protogen.GeneratedFile, f *protogen.File) error {
	for _, svc := range f.Services {
		/*
			svcOpts := svc.Desc.Options()
			if !proto.HasExtension(svcOpts, options.E_StateSelector) {
				continue
			}

			state, ok := proto.GetExtension(svcOpts, options.E_StateSelector).(string)
			if !ok {
				return errors.New("unknown state selector")
			}
		*/
		generateGrain(g, svc)
	}
	return nil
}

func generateGrain(g *protogen.GeneratedFile, svc *protogen.Service) {
	generateGrainServices(g, svc)
	writeGrainInterface(g, svc)
	generateGrainRef(g, svc)
	generateGrainDescriptor(g, svc)
	generateGrainHandlers(g, svc)
	generateGrainClient(g, svc)
}

func isObservable(m *protogen.Method) bool {
	return m.Desc.IsStreamingClient() || m.Desc.IsStreamingServer()
}

func writeGrainInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "Grain interface {")
	g.P(grainRefType)
	for _, m := range svc.Methods {
		if isObservable(m) {
			g.P(grainInterfaceRegisterObserverSignature(g, m))
		} else {
			g.P(grainInterfaceMethodSignature(g, m))
		}
	}
	g.P("}")
	g.P()
}

func grainInterfaceRegisterObserverSignature(g *protogen.GeneratedFile, m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteString("Register")
	builder.WriteString(m.GoName)
	builder.WriteString("Observer(")
	builder.WriteString("ctx ")
	builder.WriteString(g.QualifiedGoIdent(contextType))
	builder.WriteString(", ")
	builder.WriteString("observer ")
	builder.WriteString(g.QualifiedGoIdent(identityType))
	builder.WriteString(", ")
	builder.WriteString("req *")
	builder.WriteString(g.QualifiedGoIdent(m.Input.GoIdent))
	builder.WriteString(") ")
	builder.WriteString("error")
	return builder.String()
}

func grainInterfaceMethodSignature(g *protogen.GeneratedFile, m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteString(m.GoName)
	builder.WriteString("(")
	builder.WriteString("ctx ")
	builder.WriteString(g.QualifiedGoIdent(contextType))
	builder.WriteString(", ")
	builder.WriteString("req *")
	builder.WriteString(g.QualifiedGoIdent(m.Input.GoIdent))
	builder.WriteString(") ")
	builder.WriteString("(*")
	builder.WriteString(g.QualifiedGoIdent(m.Output.GoIdent))
	builder.WriteString(", error)")
	return builder.String()
}

func generateGrainRef(g *protogen.GeneratedFile, svc *protogen.Service) {
	for _, m := range svc.Methods {
		if isObservable(m) {
			generateGrainObserver(g, m)
		}
	}
	writeGrainRefInterface(g, svc)
}

func generateGrainObserver(g *protogen.GeneratedFile, m *protogen.Method) {
	writeGrainObserverInterface(g, m)
	writeGrainObserverActivator(g, m)
}

func writeGrainObserverInterface(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("type ", observerRefName(m), " interface {")
	g.P(grainRefType)
	g.P("OnNotify", m.GoName, "(",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"req *", g.QualifiedGoIdent(m.Output.GoIdent),
		") error",
	)
	g.P("}")
	g.P()
}

func observerActivatorName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteRune('_')
	builder.WriteString(m.Parent.GoName)
	builder.WriteRune('_')
	builder.WriteString(m.GoName)
	builder.WriteString("_ObserverActivator")
	return builder.String()
}

func anonymousObserverImplName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteString("impl_")
	builder.WriteString(m.Parent.GoName)
	builder.WriteString("Grain")
	builder.WriteString(m.GoName)
	builder.WriteString("Observer")
	return builder.String()
}

func writeGrainObserverActivator(g *protogen.GeneratedFile, m *protogen.Method) {

	g.P("func Create", m.Parent.GoName, "Grain", m.GoName, "Observer(",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"s *", g.QualifiedGoIdent(siloPackage.Ident("Silo")), ", ",
		"f func(", "ctx ", g.QualifiedGoIdent(contextType), ", ",
		"req *", g.QualifiedGoIdent(m.Output.GoIdent),
		") error", ") (", g.QualifiedGoIdent(grainRefType), ", error) {",
	)

	g.P("identity, err := s.CreateGrain(&", observerActivatorName(m), " {")
	g.P("f: f,")
	g.P("})")
	g.P("if err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return identity, nil")

	g.P("}")
	g.P()

	g.P("type ", anonymousObserverImplName(m), " struct {")
	g.P(g.QualifiedGoIdent(identityType))
	g.P("f func(", "ctx ", g.QualifiedGoIdent(contextType), ", ",
		"req *", g.QualifiedGoIdent(m.Output.GoIdent),
		") error")
	g.P("}")
	g.P()

	g.P("func (g *", anonymousObserverImplName(m), ") OnNotify", m.GoName, "(",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"req *", g.QualifiedGoIdent(m.Output.GoIdent),
		") error {",
	)
	g.P("return g.f(ctx, req)")
	g.P("}")

	g.P("type ", observerActivatorName(m), " struct {")
	g.P("f func(", "ctx ", g.QualifiedGoIdent(contextType), ", ",
		"req *", g.QualifiedGoIdent(m.Output.GoIdent),
		") error")
	g.P("}")
	g.P()

	g.P("func (a *", observerActivatorName(m), ") Activate(",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"identity ", g.QualifiedGoIdent(identityType), ") (",
		g.QualifiedGoIdent(grainRefType), ", error) {",
	)
	g.P("return &", anonymousObserverImplName(m), "{")
	g.P("Identity: identity,")
	g.P("f: a.f,")
	g.P("}, nil")

	g.P("}")
	g.P()
}

func writeGrainRefInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "GrainRef interface {")
	g.P(grainRefType)
	for _, m := range svc.Methods {
		if isObservable(m) {
			g.P(grainRefInterfaceObserverSignature(g, m))
		} else {
			g.P(grainInterfaceMethodSignature(g, m))
		}
	}
	g.P("}")
	g.P()
}

func observerRefName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteString(m.Parent.GoName)
	builder.WriteString("Grain")
	builder.WriteString(m.GoName)
	builder.WriteString("Observer")
	return builder.String()
}

func grainRefInterfaceObserverSignature(g *protogen.GeneratedFile, m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteString("Observe")
	builder.WriteString(m.GoName)
	builder.WriteString("(")
	builder.WriteString("ctx ")
	builder.WriteString(g.QualifiedGoIdent(contextType))
	builder.WriteString(", ")
	builder.WriteString("observer ")
	builder.WriteString(g.QualifiedGoIdent(grainRefType))
	builder.WriteString(", ")
	builder.WriteString("req *")
	builder.WriteString(g.QualifiedGoIdent(m.Input.GoIdent))
	builder.WriteString(") error")
	return builder.String()
}

func generateGrainServices(g *protogen.GeneratedFile, svc *protogen.Service) {
	writeGrainServicesInterface(g, svc)
	writeGrainServicesImplementation(g, svc)
	writeGrainActivatorInterface(g, svc)
	writeRegisterGrainActivator(g, svc)
}

func writeGrainServicesInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "GrainServices interface {")
	g.P("CoreGrainServices() ", g.QualifiedGoIdent(coreGrainServicesType))
	for _, m := range svc.Methods {
		if isObservable(m) {
			g.P("Notify", m.GoName, "Observers(",
				"ctx ", g.QualifiedGoIdent(contextType), ", ",
				"observers []", g.QualifiedGoIdent(registeredObserverType), ", ",
				"val *", g.QualifiedGoIdent(m.Output.GoIdent), ") error",
			)

			g.P("List", m.GoName, "Observers(",
				"ctx ", g.QualifiedGoIdent(contextType), ") (",
				"[]", g.QualifiedGoIdent(registeredObserverType), ", error)",
			)
			g.P("Add", m.GoName, "Observer(",
				"ctx ", g.QualifiedGoIdent(contextType), ",",
				"observer ", g.QualifiedGoIdent(identityType), ",",
				"req *", g.QualifiedGoIdent(m.Input.GoIdent), ",",
				") error",
			)
		}
	}
	g.P("}")
	g.P()
}

func writeGrainServicesImplementation(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", "impl_", svc.GoName, "GrainServices struct {")
	g.P("observerManager ", grainObserverManagerType)
	g.P("coreServices ", coreGrainServicesType)
	g.P("}")
	g.P()
	g.P("func (m *", "impl_", svc.GoName, "GrainServices) CoreGrainServices() ", coreGrainServicesType, " {")
	g.P("return m.coreServices")
	g.P("}")
	g.P()

	observerIdx := 0
	for _, m := range svc.Methods {
		if isObservable(m) {
			g.P("func (m *", "impl_", svc.GoName, "GrainServices) Notify", m.GoName, "Observers(",
				"ctx ", g.QualifiedGoIdent(contextType), ", ",
				"observers []", g.QualifiedGoIdent(registeredObserverType), ", ",
				"val *", g.QualifiedGoIdent(m.Output.GoIdent), ") error {",
			)
			g.P("return m.observerManager.Notify(",
				"ctx,",
				"ChirperGrain_GrainDesc.Observables[", observerIdx, "].Name,",
				"observers, val)",
			)
			g.P("}")
			g.P()

			g.P("func (m *", "impl_", svc.GoName, "GrainServices) List", m.GoName, "Observers(",
				"ctx ", g.QualifiedGoIdent(contextType), ") (",
				"[]", g.QualifiedGoIdent(registeredObserverType), ", error) {",
			)
			g.P("return m.observerManager.List(",
				"ctx,",
				"ChirperGrain_GrainDesc.Observables[", observerIdx, "].Name)",
			)
			g.P("}")
			g.P()
			g.P("func (m *", "impl_", svc.GoName, "GrainServices) Add", m.GoName, "Observer(",
				"ctx ", g.QualifiedGoIdent(contextType), ",",
				"observer ", g.QualifiedGoIdent(identityType), ",",
				"req *", g.QualifiedGoIdent(m.Input.GoIdent), ",",
				") error {",
			)
			g.P("_, err := m.observerManager.Add(",
				"ctx,",
				"ChirperGrain_GrainDesc.Observables[", observerIdx, "].Name,",
				"observer, req)",
			)
			g.P("return err")
			g.P("}")
			g.P()

			observerIdx++
		}
	}
	g.P()
}

func writeGrainActivatorInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "GrainActivator interface {")
	g.P("Activate(",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"identity ", g.QualifiedGoIdent(identityType), ", ",
		"services ", svc.GoName, "GrainServices", ") (",
		svc.GoName, "Grain, error)",
	)
	g.P("}")
	g.P()
}

func writeRegisterGrainActivator(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("func Register", svc.GoName, "GrainActivator(",
		"registrar ", g.QualifiedGoIdent(descriptorPackage.Ident("Registrar")), ", ",
		"activator ", svc.GoName, "GrainActivator) {",
	)
	g.P("registrar.Register(&", svc.GoName, "Grain_GrainDesc, activator)")
	g.P("}")
	g.P()
}

func generateGrainHandlers(g *protogen.GeneratedFile, svc *protogen.Service) {
	writeActivateHandler(g, svc)
	for _, m := range svc.Methods {
		if isObservable(m) {
			writeObserverHandler(g, m)
			writeRegisterObserverHandler(g, m)
		} else {
			writeMethodHandler(g, m)
		}
	}
}

func activatorHandlerName(svc *protogen.Service) string {
	builder := strings.Builder{}
	builder.WriteRune('_')
	builder.WriteString(svc.GoName)
	builder.WriteString("Grain_Activate")
	return builder.String()
}

func writeActivateHandler(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("func ", activatorHandlerName(svc), "(",
		"activator interface{},",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"coreServices ", g.QualifiedGoIdent(coreGrainServicesType), ", ",
		"observerManager ", g.QualifiedGoIdent(grainObserverManagerType), ", ",
		"identity ", g.QualifiedGoIdent(identityType), ") (",
		g.QualifiedGoIdent(grainRefType), ", error) {",
	)

	g.P("grainServices := &", "impl_", svc.GoName, "GrainServices {")
	g.P("observerManager: observerManager,")
	g.P("coreServices: coreServices,")
	g.P("}")
	g.P("return activator.(", svc.GoName, "GrainActivator)", ".Activate(ctx, identity, grainServices)")

	g.P("}")
	g.P()
}

func observerHandlerName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteRune('_')
	builder.WriteString(m.Parent.GoName)
	builder.WriteString("Grain_")
	builder.WriteString(m.GoName)
	builder.WriteString("_ObserverHandler")
	return builder.String()
}

func registeObserverHandlerName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteRune('_')
	builder.WriteString(m.Parent.GoName)
	builder.WriteString("Grain_")
	builder.WriteString(m.GoName)
	builder.WriteString("_RegisterObserverHandler")
	return builder.String()
}

func writeObserverHandler(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("func ", observerHandlerName(m), "(",
		"srv interface{},",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"dec func(interface{}) error) error {",
	)
	g.P("in := new(", g.QualifiedGoIdent(m.Output.GoIdent), ")")

	g.P("if err := dec(in); err != nil {")
	g.P("return err")
	g.P("}")
	g.P()
	g.P("return srv.(", observerRefName(m), ")", ".OnNotify", m.GoName, "(ctx, in)")

	g.P("}")
}

func writeRegisterObserverHandler(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("func ", registeObserverHandlerName(m), "(",
		"srv interface{},",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"observer ", g.QualifiedGoIdent(identityType), ", ",
		"dec func(interface{}) error) error {",
	)
	g.P("in := new(", g.QualifiedGoIdent(m.Input.GoIdent), ")")

	g.P("if err := dec(in); err != nil {")
	g.P("return err")
	g.P("}")
	g.P()
	g.P("return srv.(", m.Parent.GoName, "Grain).Register", m.GoName, "Observer(ctx, observer, in)")

	g.P("}")
}

func methodHandlerName(m *protogen.Method) string {
	builder := strings.Builder{}
	builder.WriteRune('_')
	builder.WriteString(m.Parent.GoName)
	builder.WriteString("Grain_")
	builder.WriteString(m.GoName)
	builder.WriteString("_MethodHandler")
	return builder.String()
}

func writeMethodHandler(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("func ", methodHandlerName(m), "(",
		"srv interface{},",
		"ctx ", g.QualifiedGoIdent(contextType), ", ",
		"dec func(interface{}) error) (interface{}, error) {",
	)
	g.P("in := new(", g.QualifiedGoIdent(m.Input.GoIdent), ")")

	g.P("if err := dec(in); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P()
	g.P("return srv.(", m.Parent.GoName, "Grain).", m.GoName, "(ctx, in)")

	g.P("}")
	g.P()
}

func generateGrainDescriptor(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("var ", svc.GoName, "Grain_GrainDesc = ", g.QualifiedGoIdent(descriptorPackage.Ident("GrainDescription")), "{")
	g.P("GrainType: ", "\"", svc.GoName, "Grain", "\",")

	g.P("Activation: ", g.QualifiedGoIdent(descriptorPackage.Ident("ActivationDesc")), "{")
	g.P("Handler: ", activatorHandlerName(svc), ",")
	g.P("},")

	g.P("Methods: []", g.QualifiedGoIdent(descriptorPackage.Ident("MethodDesc")), "{")
	for _, m := range svc.Methods {
		if !isObservable(m) {
			writeMethodDesc(g, m)
		}
	}
	g.P("},")

	g.P("Observables: []", g.QualifiedGoIdent(descriptorPackage.Ident("ObservableDesc")), "{")
	for _, m := range svc.Methods {
		if isObservable(m) {
			writeObservableDesc(g, m)
		}
	}
	g.P("},")

	g.P("}")
	g.P()
}

func writeMethodDesc(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("{")
	g.P("Name: \"", m.GoName, "\",")
	g.P("Handler: ", methodHandlerName(m), ",")
	g.P("},")
}

func writeObservableDesc(g *protogen.GeneratedFile, m *protogen.Method) {
	g.P("{")
	g.P("Name: \"", m.GoName, "\",")
	g.P("Handler: ", observerHandlerName(m), ",")
	g.P("RegisterHandler: ", registeObserverHandlerName(m), ",")
	g.P("},")
}

func clientName(svc *protogen.Service) string {
	builder := strings.Builder{}
	builder.WriteString("_grainClient_")
	builder.WriteString(svc.GoName)
	builder.WriteString("Grain")
	return builder.String()
}

func generateGrainClient(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", clientName(svc), " struct {")
	g.P(identityType)
	g.P("siloClient ", g.QualifiedGoIdent(siloClientType))
	g.P("}")

	g.P("func Get", svc.GoName, "Grain(",
		"siloClient ", g.QualifiedGoIdent(siloClientType), ", ",
		"identity ", g.QualifiedGoIdent(identityType),
		") ChirperGrainRef {",
	)
	g.P("return &", clientName(svc), "{")
	g.P("Identity: identity,")
	g.P("siloClient: siloClient,")
	g.P("}")

	g.P("}")
	g.P()

	methodIdx := 0
	observableIdx := 0
	for _, m := range svc.Methods {
		if isObservable(m) {
			writeGrainClientRegisterObserver(g, observableIdx, m)
			observableIdx++
		} else {
			writeGrainClientMethod(g, methodIdx, m)
			methodIdx++
		}
	}
}

func writeGrainClientMethod(g *protogen.GeneratedFile, methodIdx int, m *protogen.Method) {
	g.P("func (c *", clientName(m.Parent), ") ", grainInterfaceMethodSignature(g, m), "{")
	g.P("f := c.siloClient.InvokeMethod(ctx, c.Identity,", m.Parent.GoName, "Grain_GrainDesc.GrainType, ", m.Parent.GoName, "Grain_GrainDesc.Methods[", methodIdx, "].Name, req)")

	g.P("resp, err := f.Await(ctx)")
	g.P("if err != nil {")
	g.P("return nil, err")
	g.P("}")

	g.P("out := new(", g.QualifiedGoIdent(m.Output.GoIdent), ")")
	g.P("if err := resp.Get(out); err != nil {")
	g.P("return nil, err")
	g.P("}")

	g.P("return out, nil")

	g.P("}")
}

func writeGrainClientRegisterObserver(g *protogen.GeneratedFile, observableIdx int, m *protogen.Method) {
	//grainRefInterfaceObserverSignature
	g.P("func (c *", clientName(m.Parent), ") ", grainRefInterfaceObserverSignature(g, m), "{")
	g.P("f := c.siloClient.RegisterObserver(ctx, observer.GetIdentity(), c.GetIdentity(), ", m.Parent.GoName, "Grain_GrainDesc.Observables[0].Name, req)")

	g.P("err := f.Await(ctx)")
	g.P("if err != nil {")
	g.P("return err")
	g.P("}")

	g.P("return nil")

	g.P("}")

}

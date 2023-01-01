{{ define "ObserverBatchInvoke" }}
{{ $grainType := .Name }}
{{ range .Methods }}
func {{ $grainType }}{{ .Name }}InvokeBatch(ctx context.Context, siloClient __grain.SiloClient, observerList []{{ $grainType }}, {{ template "methodParametersWithoutCtx" . }}) {
	idents := make([]__grain.Identity, len(observerList))
	for i, o := range observerList {
		idents[i] = o.GetIdentity()
	}
	siloClient.InvokeOneWayMethod(ctx, idents, "{{ .Name }}", func(respSerializer __grain.Serializer) error {
        {{ template "serializerFuncBody" . }}
	})
}
{{ end }}
{{ end }}

{{ define "GenericObserver" }}
{{ $observerType := .Name }}

{{ range .Methods }}
type {{ $observerType }}On{{ .Name }}Func func(ctx context.Context, sender __grain.Identity, {{ template "methodParametersWithoutCtx" . }})
{{ end }}

type Generic{{ $observerType }}Builder struct {
    {{ range .Methods }}
    on{{ .Name }} {{ $observerType }}On{{ .Name }}Func
    {{ end }}
}


func NewGeneric{{ $observerType }}Builder() *Generic{{ $observerType }}Builder {
	return &Generic{{ $observerType }}Builder{
        {{ range .Methods }}
        on{{ .Name }}: func(ctx context.Context, sender __grain.Identity, {{ template "methodParametersWithoutCtx" . }}) {},
        {{ end }}
	}
}

{{ range .Methods }}
func (__b *Generic{{ $observerType }}Builder) On{{ .Name }}(f {{ $observerType }}On{{ .Name }}Func) *Generic{{ $observerType }}Builder {
	__b.on{{.Name}} = f
	return __b
}
{{ end }}

func (__b *Generic{{ $observerType }}Builder) Build(g *__generic.Grain) ({{ $observerType }}, error) {
    {{ range .Methods }}
	g.OnMethod(
		"{{ .Name }}",
		func(ctx context.Context, siloClient __grain.SiloClient, method string, sender __grain.Identity, dec __grain.Deserializer, respSerializer __grain.Serializer) error {
			var err error
			{{template "decodeMethodArgs" . }}
			__b.on{{ .Name }}(ctx, sender, {{ template "callArgsWithoutCtx" . }})
			return nil
		})
    {{ end }}
	return generic{{ $observerType }}{g}, nil
}

type generic{{ $observerType }} struct {
	*__generic.Grain
}

{{ range .Methods }}
func (generic{{ $observerType }}) {{.Name}}({{ template "methodParameters" . }}) {
	panic("unreachable")
}
{{ end }}

{{ end }}

{{ define "Observer" }}
{{ template "GrainClient" . }}
{{ template "ObserverBatchInvoke" . }}
{{ template "GenericObserver" . }}
{{ end }}

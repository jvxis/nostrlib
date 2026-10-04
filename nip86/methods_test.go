package nip86

import "testing"

func TestUnknownMethodComesThroughAsGeneric(t *testing.T) {
	mp, err := DecodeRequest(Request{Method: "listarrivals", Params: []any{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	generic, ok := mp.(Generic)
	if !ok || generic.MethodName() != "listarrivals" || len(generic.Params) != 1 {
		t.Fatalf("got %#v", mp)
	}
	if _, err := DecodeRequest(Request{}); err == nil {
		t.Fatal("a request without a method was accepted")
	}
	if _, ok := mustDecode(t, Request{Method: "listallowedpubkeys"}).(ListAllowedPubKeys); !ok {
		t.Fatal("a known method stopped decoding to its own type")
	}
}

func mustDecode(t *testing.T, req Request) MethodParams {
	t.Helper()
	mp, err := DecodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	return mp
}

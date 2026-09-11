package main

import "testing"

func withFlagValue(t *testing.T, flagVar *string, value string) {
	t.Helper()
	orig := *flagVar
	*flagVar = value
	t.Cleanup(func() { *flagVar = orig })
}

func TestDBDSN(t *testing.T) {
	withFlagValue(t, dbHostFlag, "dbhost")
	withFlagValue(t, dbPortFlag, "1111")
	withFlagValue(t, dbUserFlag, "u")
	withFlagValue(t, dbPasswordFlag, "p")
	withFlagValue(t, dbNameFlag, "n")
	withFlagValue(t, dbSSLModeFlag, "require")

	want := "host=dbhost port=1111 user=u password=p dbname=n sslmode=require"
	if got := dbDSN(); got != want {
		t.Errorf("dbDSN() = %q, want %q", got, want)
	}
}

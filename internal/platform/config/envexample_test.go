package config_test

import (
	"os"
	"regexp"
	"testing"
)

// TestComposePassesEveryVariableOfEnvExample checks that every variable .env.example documents, set or
// commented out, reaches a service: Compose only substitutes ${NAME} where docker-compose.yml names it,
// so a variable missing there would be silently ignored when set in .env.
func TestComposePassesEveryVariableOfEnvExample(t *testing.T) {
	example, err := os.ReadFile("../../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	compose, err := os.ReadFile("../../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	names := regexp.MustCompile(`(?m)^#?\s?([A-Z][A-Z0-9_]*)=`).FindAllStringSubmatch(string(example), -1)
	if len(names) == 0 {
		t.Fatal(".env.example documents no variables")
	}
	for _, m := range names {
		// ${NAME}, ${NAME:-default} or ${NAME-default}, and not a longer name that starts with NAME.
		if !regexp.MustCompile(`\$\{` + regexp.QuoteMeta(m[1]) + `[}:-]`).Match(compose) {
			t.Errorf("%s is documented in .env.example but docker-compose.yml does not pass it", m[1])
		}
	}
}

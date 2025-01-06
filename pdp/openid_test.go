package pdp

import (
	"fmt"
	"testing"
)

func TestDOME_JWKS(t *testing.T) {

	t.Run("ptest", func(t *testing.T) {
		got, err := DOME_JWKS()
		if err != nil {
			t.Errorf("DOME_JWKS() error = %v", err)
			return
		}
		fmt.Println(got)
	})
}

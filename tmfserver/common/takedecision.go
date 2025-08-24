package common

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/hesusruiz/domepdp/pdp"
	"github.com/hesusruiz/domepdp/tmfserver/repository"
	"gitlab.com/greyxor/slogor"
)

func takeDecision(
	ruleEngine *pdp.PDP,
	r *Request,
	tokenClaims map[string]any,
	tmfObject *repository.TMFObject,
) bool {
	// Assemble all data in a single "input" argument, to the style of OPA.
	// We mutate the predeclared identifier, so the policy can access the data for this request.
	// We can also service possible callbacks from the rules engine.

	userArgument := pdp.StarTMFMap(r.AuthUser.ToMap())
	tmfObjectArgument := pdp.StarTMFMap(tmfObject.ToMap())
	requestArgument := pdp.StarTMFMap(r.ToMap())
	tokenArgument := pdp.StarTMFMap(tokenClaims)

	input := map[string]any{
		"request": requestArgument,
		"token":   tokenArgument,
		"tmf":     tmfObjectArgument,
		"user":    userArgument,
	}

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		b, err := json.MarshalIndent(input, "", "  ")
		if err == nil {
			fmt.Println("PDP input:", string(b))
		}
	}

	var err error

	decision := true
	if ruleEngine != nil {
		decision, err = ruleEngine.TakeAuthnDecision(pdp.Authorize, input)

		// An error is considered a rejection, continue with the next candidate object
		if err != nil {
			slog.Error("PDP: request rejected due to an error", slogor.Err(err))
			return false
		}
	}

	// The rules engine rejected the request, continue with the next candidate object
	if !decision {
		slog.Warn("PDP: request rejected due to policy")
		return false
	}

	// The rules engine accepted the request, add the object to the final list
	slog.Info("PDP: request authorised")
	return true
}

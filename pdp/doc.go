// Package pdp implements a Policy Decision Point (PDP) for access control policies for TMForum objects.
//
// The PDP evaluates access requests against a set of policies and returns a
// decision (Permit, Deny).
//
// The decision is taken based on data for the incoming request, the user making the request
// and the policy information inside the target TMForum object as a set of 'Terms of Use'.
//
// The package provides functions for loading policies, creating requests,
// and evaluating requests against the loaded policies.
//
// [TMForum]: https://www.tmforum.org/oda/open-apis/directory
package pdp

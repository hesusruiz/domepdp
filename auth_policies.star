"""
This module defines a funtion called 'authorize', which is called when
a user/machine tries to access a protected resource.

The function determines if the request is allowed and must reply
True (allowed) or False (denied).

The 'authorize' function has access to an object called 'input' which contains
four objects that can be used to implement the authorization policies: 'request', 'token', 'user' and 'tmf':

"request" is an object with the following fields representing the HTTP request received:

    "action": one of 'LIST', 'READ', 'CREATE', 'UPDATE' or 'DELETE', an alias of the HTTP method of the request.
    "method": the HTTP method that was used in the request ('GET', 'POST', 'PUT', 'PATCH' or 'DELETE').
    "host": the host header in the request.
    "remote_addr": the IP address of the remote machine accessing the object.
    "path": the url path (does not include the query parameters).
    "query": a dictionary with all the query parameters in the url.

    "resource": the TMForum resource being accessed (eg., productOffering, catalog, etc.).
    "id": the identifier of the TMForum object being accessed.

    "headers": a dictionary with the headers in the HTTP request.

"token" is an object with the contents of the Access Token received with
    the request. The most important object inside the 'token' object is
    the LEARCredential, accessed via the 'vc' property of 'token'.
    The Access Token has already been formally verified, including that the signature is valid.

    "vc": contains the LEARCredential presented by the caller. The most important
        sub-objects in 'vc' are the 'mandator', 'mandatee' and 'powers', which
        can be used by the 'authorize' function to implement the policies.

"user" is an object with some properties extracted from the token to facilitate writing
    rules. It is just a convenience object and the rules can always access the token if needed.

    "isAuthenticated" is boolean which is True if the request came with a valid access token.
    "organizationIdentifier" is the identifier for the mandator.
    "isLEAR" is a boolean which is True if the user has the "Onboarding" power.
    "isOwner" is a boolean which is True if the TMF object is owned by the organization of the user.
    "country" is the two letter code for the country of incorporation of the organization.
    "isSeller", "isBuyer", "isSellerOperator" and "isBuyerOperator" are booleans reporting if the 
        user has that role in the TMF object being accessed

"tmf" has the contents of the TMForum object that the remote user tries to access.
    The policies can access any component of the object, but to simplify writing policy rules,
    the system makes available some calculated fist level sub-objects inside the 'tmf' object:

    "resource": the resource name of the TMForum object being accessed, like 'productOffering' or 'productSpecification'.
    "organizationIdentifier": the identifier of the company who owns the TMForum object,
        which is the company that created the object in the DOME Marketplace.
    "permittedCountries" and "prohibitedCountries" which are lists of countries according to the
        country restriction policies embedded in the TMForum object.
    "permittedOperators" and "prohibitedOperators" which are lists of operator identities according to the
        operator restriction policies embedded in the TMForum object.

The policies below are an example that can be used as starting point by the policy writer.
They can be customized as needed, using the data in the 'input' object for making
the authorization decision.
"""

allowed_countries = ["",
    "ES", "FR", "IT", "DE", "PT", "UK", "IE", "NL", "BE", "LU", "AT", "CH",
    "SE", "NO", "FI", "DK", "PL", "CZ", "SK", "HU", "RO", "BG", "GR", "TR",
    "RU", "UA", "BY", "LT", "LV", "EE", "HR", "SI", "RS", "BA", "MK", "AL",
    "XK", "ME", "MD", "IS", "FO", "GL", "GI", "MT", "CY", "LI", "AD", "MC",
    "SM", "VA", "JE", "GG", "IM"]

forbidden_countries = ["RU"]

# These are just examples of policies that you can use to define yours.
# You can add and delete anything that you need for implementing your policies.
# The only thing you can not change is the name of the function,
# which must be 'authorize'.
def authorize():

    # The 'print' function writes to the logging system 
    print("Inside authorize for", input.request.action)
    if input.user.isAuthenticated:
        print("user:", input.user.organizationIdentifier, "Is LEAR?", input.user.isLEAR)
    else:
        print("user is not authenticated")


    # This rule denies access to remote users belonging to an
    # organization in the list of forbidden countries
    if input.user.country in forbidden_countries:
        print("rejected because country forbidden:", input.user.country)
        return False

    # This rule denies access to remote users not explicitly included
    # in the allowed countries list
    if input.user.country not in allowed_countries:
        print("forbidden because country not allowed:", input.user.country)
        return False

    # You can take different decisions depending on the action that the
    # user is intending to do
    if input.request.action == "UPDATE":
        return True

    # This denies access to all requests that have not been rejected or
    # accepted by the previous rules.
    # The default is to deny access, so if you do not explicitly return True
    # at some point, the request will be rejected.
    return True

    # *********************************************************************
    # The previous statement ('return') stops evaluation of rules.
    # The rules below are additional examples of fields available for rules.
    # They are not executed but you can copy/paste and adapt.
    # *********************************************************************

    # This rule denies access if the organization of the remote user is not
    # the same as the one owning the TMForum object
    if not input.user.isOwner:
        return False

    # You can also access the powers of the remote user,
    # available in the LEARCredential.
    # You can use variables to facilitate writing the rules. For example:

    mandator = input.token.vc.credentialSubject.mandate.mandator
    mandatee = input.token.vc.credentialSubject.mandate.mandatee
    powers = input.token.vc.credentialSubject.mandate.power

    remote_user_organization = mandator.organizationIdentifier

    # An alternative syntax is available if you are more comfortable with it
    credential_subject = input["token"]["vc"]["credentialSubject"]
    also_the_mandator = credential_subject["mandate"]["mandator"]
    also_the_user_organization = also_the_mandator["organizationIdentifier"]

    # This is just in case we reach here for some reason
    return False



#############################################################
# Auxiliary functions
#############################################################

# Policies can be as complex as you want, and functions can help to structure them.
# This is an example of a function that can be used to abstract some policy rules and
# facilitates reuse in your main rules section.
def credentialIncludesPower(credential, action, function, domain):
    """credentialIncludesPower determines if a given power is incuded in the credential.

    Args:
        credential: the received credential.
        action: the action that should be allowed.
        function: the function that should be allowed.
        domain: the domain that should be allowed.

    Returns:
        True or False, for allowing authentication or denying it, respectively.
    """

    # Get the POWERS information from the credential
    powers = credential["verifiableCredential"]["credentialSubject"]["mandate"]["power"]

    # Check all possible powers in the mandate
    for power in powers:
        # Approve if the power includes the required one
        if (power["tmf_function"] == function) and (domain in power["tmf_domain"]) and (action in power["tmf_action"]):
            return True

    # We did not find any complying power, so Deny
    return False


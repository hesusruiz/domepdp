"""
This module should define two functions: 'authenticate' and 'authorize'.
- 'authenticate' is called when performing authentication, and
- 'authorize' is called when actually accessing a protected resource.

Each of those functions should determine if the request is allowed and reply
True (allowed) or False (denied).

The 'authenticate' and 'authorize' functions receive three objects: 'request', 'rawcred' and 'protected_resource'

"request" is a dictionary with the following fields:
    "method": the HTTP method that was used in teh request
    "host": the host header in the request
    "remoteip": the IP of the remote host sending the request
    "url": the complete url of the request
    "path": the url path until the query parameters
    "protocol": the 'http' or 'https' protocol
    "headers": a dictionary with the headres in the HTTP request
    "pathparams": a dictionary wil all the path parameters
    "queryparams": a dictionary with all the query parameters in the url

'rawcred' is a JSON string serialization of the Verifiable Credential received in the request.
"""

allowed_countries = ("ES", "FR", "IT", "DE", "PT", "UK", "IE", "NL", "BE", "LU", "AT", "CH", "SE", "NO", "FI", "DK", "PL", "CZ", "SK", "HU", "RO", "BG", "GR", "TR", "RU", "UA", "BY", "LT", "LV", "EE", "HR", "SI", "RS", "BA", "MK", "AL", "XK", "ME", "MD", "IS", "FO", "GL", "GI", "MT", "CY", "LI", "AD", "MC", "SM", "VA", "JE", "GG", "IM")

forbidden_countries = ()

def authorize(request, claims, tmf):
    """authenticate determines if a user can be authenticated or not.

    Args:
        request: the HTTP request received.
        rawcred: the raw credential encoded in string format.

    Returns:
        True or False, for allowing authentication or denying it, respectively.
    """
    print("Inside authenticate")

    print("type:", tmf["type"])
    print("owner:", tmf["organizationIdentifier"])
    print("user:", request["user"])

    if not request["user"]["isOwner"]:
        return False

    method = request["method"]
    if method == "GET":
        return True

    mandator_country = claims["verifiableCredential"]["credentialSubject"]["mandate"]["mandator"]["country"]
    
    if mandator_country in forbidden_countries:
        print(mandator_country, "is Forbidden country")
        return False
    if mandator_country not in allowed_countries:
        print(mandator_country, "is Not allowed country")
        return False

    print(mandator_country, "is Allowed country")
    return True



###############################################################################
# Auxiliary functions
###############################################################################


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


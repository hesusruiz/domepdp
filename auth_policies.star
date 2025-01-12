"""
This module defines a funtion called 'authorize', which is called when a user/machine tries to access a protected resource.

The function determines if the request is allowed and must reply True (allowed) or False (denied).

The 'authorize' function has access to an object called 'input' which contains three objects that can be used to implement the authorization policies: 'request', 'token' and 'tmf':

"request" is a dictionary with the following fields representing the HTTP request received:
    "action": one of 'CREATE', 'READ', 'UPDATE' or 'DELETE'.
    "tmf_entity": the TMForum entity being accessed (eg., productOffering, catalog, etc.)
    "tmf_id": the identifier of the TMForum object being accessed
    "remote_addr": the IP address of the remote machine acessing the object
    "method": the HTTP method that was used in the request ('GET', 'POST', 'PUT', 'PATCH' or 'DELETE').
    "host": the host header in the request.
    "remoteip": the IP of the remote host sending the request
    "url": the complete url of the request
    "path": the url path until the query parameters
    "protocol": the 'http' or 'https' protocol
    "headers": a dictionary with the headers in the HTTP request
    "query": a dictionary with all the query parameters in the url

"token" is a dictionary with the contents of the Access Token received with the request. The most important object inside the 'token' object is the LEARCredential, accessed via the 'vc' property of 'token'. The Access Token has already been formally verified, including that the signature is valid.
    "vc": contains the LEARCredential presented by the caller. The most important sub-objects in 'vc' are the 'mandator', 'mandatee' and 'powers', which can be used by the 'authorize' function to implement the policies.

"tmf" has the contents of the TMForum object that the remote user tries to access. This can be used by the policies to determine if access is granted or not. To simplify writing policy rules, the first level sub-objects inside the 'tmf' object include, among others:
    "type": the type of TMForum being accessed, like 'productOffering' or 'productSpecification'.
    "organizationIdentifier": the identifier of the company who owns the TMForum object, which is the company that created the object in the DOME Marketplace.

The policies below are an example that can be used as starting point by the policy writer. They can be customized as needed, using the data in the 'input' object for making the authorization decision.
"""

allowed_countries = ("ES", "FR", "IT", "DE", "PT", "UK", "IE", "NL", "BE", "LU", "AT", "CH", "SE", "NO", "FI", "DK", "PL", "CZ", "SK", "HU", "RO", "BG", "GR", "TR", "RU", "UA", "BY", "LT", "LV", "EE", "HR", "SI", "RS", "BA", "MK", "AL", "XK", "ME", "MD", "IS", "FO", "GL", "GI", "MT", "CY", "LI", "AD", "MC", "SM", "VA", "JE", "GG", "IM")

forbidden_countries = ("RU")

def authorize():
    print("Inside authorize")

    # This rule denies access to remote users belonging to an organization in the list of forbidden countries
    if input.request.country in forbidden_countries:
        return False

    # This rule denies access to remote users not explicitly included in the allowed countries list
    if input.request.country not in allowed_countries:
        return False

    # This allows access to all requests that have not been rejected by the previous rules.
    # The default is to deny access, so if you do not explicitly return True at some point, the request will be rejected.
    return True

    # The previous rule stops evaluation of rules beyond this point. The rules below are examples of what fields are
    # available for the rules.

    # This rule denies access if the organization of the remote user is not the same as the one owning the TMForum object
    if not input.remote_user.isOwner:
        return False

    # The above rule uses convenience properties made available to the rule engine to simplify writing the rules.
    # That rule is equivalent to the following:
    if input.request.organizationIdentifier != input.tmf.owner.organizationIdentifier:
        return False

    # You can also access the powers of the remote user, available in the LEARCredential. You can use variables to facilitate writing the rules. Also, in addition to being used in rule evaluation, properties can be written in the access log relevant information using the 'print' function. For example:

    mandator = input.token.vc.credentialSubject.mandate.mandator
    mandatee = input.token.vc.credentialSubject.mandate.mandatee
    powers = input.token.vc.credentialSubject.mandate.power

    remote_user_organization = input.token.vc.credentialSubject.mandate.mandator.organizationIdentifier
    print("organization of remote user:", remote_user_organization)

    powers = inp
    print("owner:", tmf.organizationIdentifier)
    print("user:", request["user"])

#    if not request["user"]["isOwner"]:
#        return False

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


export const serverConfig = {
  port() { return process.env.PORT || 5000; },

  authServiceName() {
    return process.env.AUTH_SERVICE_NAME;
  },

  keycloakServiceUrl() {
    return process.env.KEYCLOAK_SERVICE_URL;
  },

  keycloakServiceName() {
    return process.env.KEYCLOAK_SERVICE_NAME;
  },

  clientId() {
    return process.env.CLIENT_ID
  },

  clientSecret() {
    return process.env.CLIENT_SECRET;
  }
};
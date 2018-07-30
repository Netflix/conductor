const axios = require('axios');

class BaseService {
  constructor(token) {
    const { WEBSITE_HOSTNAME } = process.env;

    this.hostName = `http://${WEBSITE_HOSTNAME}`;

    // Set config defaults when creating the instance
    this.config = {
      method: 'POST',
      headers: { Authorization: token }
    };
  }

  handleError = e => {
    // eslint-disable-next-line no-console
    console.error(e);
    return e;
  };

  async get(url) {
    try {
      return await axios({ ...this.config, method: 'GET', url });
    } catch (e) {
      return this.handleError(e);
    }
  }

  async post(url, data) {
    try {
      return await axios({ ...this.config, method: 'POST', url, data });
    } catch (e) {
      return this.handleError(e);
    }
  }

  async put(url, data) {
    try {
      return await axios({ ...this.config, method: 'PUT', url, data });
    } catch (e) {
      return this.handleError(e);
    }
  }

  async delete(url) {
    try {
      return await axios({ ...this.config, method: 'DELETE', url });
    } catch (e) {
      return this.handleError(e);
    }
  }
}

module.exports = BaseService;

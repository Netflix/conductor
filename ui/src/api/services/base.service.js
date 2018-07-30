const axios = require('axios');

class BaseService {
  constructor(token) {
    const { WF_SERVER } = process.env;
    this.hostName = WF_SERVER;

    // Set config defaults when creating the instance
    this.config = {};

    if (token) {
      this.config.headers = { Authorization: token };
    }
  }

  handleError = e => {
    // eslint-disable-next-line no-console
    console.error(e);
    return e;
  };

  async get(url) {
    try {
      const { data } = await axios({ ...this.config, method: 'GET', url: `${this.hostName}${url}` });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async post(url, body) {
    try {
      const { data } = await axios({ ...this.config, method: 'POST', url: `${this.hostName}${url}`, data: body });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async put(url, body) {
    try {
      const { data } = await axios({ ...this.config, method: 'PUT', url: `${this.hostName}${url}`, data: body });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async delete(url) {
    try {
      const { data } = await axios({ ...this.config, method: 'DELETE', url: `${this.hostName}${url}` });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }
}

module.exports = BaseService;

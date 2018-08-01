import axios from 'axios';

class BaseService {
  constructor() {
    const { WF_SERVER } = process.env;
    this.hostName = WF_SERVER;
  }

  handleError = e => {
    // eslint-disable-next-line no-console
    console.error(e);
    return e;
  };

  config = token => (token ? { Authorization: token } : {});

  async get(url, token) {
    try {
      const { data } = await axios({ ...this.config(token), method: 'GET', url: `${this.hostName}${url}` });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async post(url, body, token) {
    try {
      const { data } = await axios({
        ...this.config(token),
        method: 'POST',
        url: `${this.hostName}${url}`,
        data: body
      });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async put(url, body, token) {
    try {
      const { data } = await axios({ ...this.config(token), method: 'PUT', url: `${this.hostName}${url}`, data: body });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }

  async delete(url, token) {
    try {
      const { data } = await axios({ ...this.config(token), method: 'DELETE', url: `${this.hostName}${url}` });
      return data;
    } catch (e) {
      return this.handleError(e);
    }
  }
}

module.exports = BaseService;

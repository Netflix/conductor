import BaseService from './base.service';

class MetadataService extends BaseService {
  constructor() {
    super();

    const { META_API_BASE_ROUTE = 'metadata' } = process.env;
    this.baseRoute = META_API_BASE_ROUTE;
  }

  async list(token) {
    const result = await this.get(`${this.baseRoute}/workflow`, token);

    return { result };
  }

  async getByName(name, version, token) {
    const result = await this.get(`${this.baseRoute}/workflow/${name}?version=${version}`, token);

    return { result };
  }

  async taskDef(token) {
    const result = await this.get(`${this.baseRoute}/taskdefs`, token);

    return { result };
  }
}

module.exports = MetadataService;

import BaseService from './base.service';

class SystemService extends BaseService {
  async adminConfig(token) {
    const { data: config } = await this.get('admin/config', token);

    const result = {
      ...config,
      server: process.env.WF_SERVER,
      env: process.env
    };
    return { sys: result };
  }
}

module.exports = SystemService;

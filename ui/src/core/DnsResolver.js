import dns from 'dns';
import log4js from 'log4js';

const logger = log4js.getLogger('server.dnsResolver');

class DnsResolver {
  resolve(host, success, error) {
    if (this.isIpAndPortFormat(host))
      success(this.parseIpAndPortFormat(host));
    else
      dns.resolveSrv(host, (err, addresses) => {
        if (err) {
          logger.error("Error: " + err);
          error({error: err})
        } else {
          success(addresses);
        }
      });
  }

  isIpAndPortFormat(host) {
    // simplistic ipv4 format ip address and port (000.000.000.000:00000)
    return /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]+$/.test(host);
  }

  parseIpAndPortFormat(host) {
    const tokens = host.split(':');
    return [{
      name: tokens.length >= 1 ? tokens[0] : null,
      port: tokens.length >= 2 ? parseInt(tokens[1]) : null,
      priority: 1,
      weight: 1
    }];
  }
}

export default DnsResolver;
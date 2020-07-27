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
          logger.error("resolveSrv error: " + err);
          error({error: err})
        } else {
          let instance = addresses[0];
          dns.resolve4(instance.name, (err2, ipaddress) => {
            if (err2) {
              logger.error("resolve4 error: " + err2);
              error({error: err2})
            } else {
              success([{'name': ipaddress, 'port': instance.port}]);
            }
          });
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

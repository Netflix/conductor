import dns from 'dns';

const wfService = process.env.WF_SERVICE;

const ApiLookup = {

  lookup: () => new Promise((resolve, reject) => {
    if (!wfService) {
      resolve(process.env.WF_SERVER);
      return;
    }

    dns.resolveSrv(wfService, function (err, addresses) {
      if (err) {
        reject(err);
      } else if (addresses && addresses.length > 0) {
        let instance = addresses[0];
        resolve('http://' + instance.name + ':' + instance.port + '/api/');
      } else {
        reject('No service found');
      }
    });
  })
};

export default ApiLookup;
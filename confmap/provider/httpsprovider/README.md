What is this new component httpsprovider?
- An implementation of `confmap.Provider` for HTTPS (httpsprovider) allows OTEL Collector the ability to load configuration for itself by fetching and reading config files stored in HTTPS servers.

How this new component httpsprovider works?
- It will be called by `confmap.Resolver` to load configurations for OTEL Collector.
- By giving a config URI starting with prefix 'https://', this httpsprovider will be used to download config files from given HTTPS URIs, and then used the downloaded config files to deploy the OTEL Collector.
- In our code, we check the validity scheme and string pattern of HTTP URIs. And also check if there are any problems on config downloading and config deserialization.
- Make sure that the certificates provided by HTTPS servers are from trusted CAs. Otherwise, customers need to set up the env SSL_CERT_FILE with the path of the CA certificate, this allows our code to manually consider the CA a trusted one.

Expected URI format:
- http://...

Prerequistes:
- Need to setup a HTTP server ahead, which returns with a config files according to the given URI
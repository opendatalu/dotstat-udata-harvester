# dotstat-udata-harvester
an harvester for the [.stat suite](https://siscc.org/stat-suite/).

This harvester will download the metadata from a .stat instance and create datasets on an [udata-based open data portal](https://github.com/opendatateam/udata).
It is currently used to synchronize [data.public.lu](https://data.public.lu) with the [LUSTAT database](https://lustat.statec.lu/).

## Configuration

Copy the `.env-template` file into a file named `.env`. Adjust the following variales to your needs:

- odpURL: URL of the udata instance
- odpAPIKey: API key needed to access the udata API
- orgId: id of the organization on udata which will publish the datasets
- dotstatURL: URL of the .stat instance
- dotstatDataflowURLPrefix: prefix of the URLs which will be needed to generate links towards dataflows. The id of the dataflow is appended at the end. Ex: `https://lustat.statec.lu/vis?df[ds]=release&df[ag]=LU1&df[id]=`
- license: a valid license code as configured on your udata instance
- syncTag: tag used on the udata instance to identify all synchronized datasets. Ex: "dotstat-sync"
- dotstatMainFacet: name of your main .stat facet. Ex: "Themes"
- dotstatDatasourceId: .stat datasource id.
- dotstatLang: .stat language code. Ex: "en"
- tenant: the .stat tenant. Ex: "default"
- descTemplate: template file to be used to generate a description for each dataset. This template is in ejs format. By default, `desc.ejs` will be used.
- callRateNrCalls: this setting and the following are related to rate limiting. This is the max number of calls per period. By default 1.
- callRateDuration: this setting defines the duration of the period for rate limiting in milliseconds. By default 1000ms.

## Run

You can launch the synchronization with the command `npm run main`.
The script named `run-win.sh` launches the synchronization on Windows and creates a log file. Bash.exe is needed, it can be found in [git for Windows](https://git-scm.com/download/win).

## License
This software is (c) [Information and press service](https://sip.gouvernement.lu/en.html) of the luxembourgish government and licensed under the MIT license.

import { getSyncedDatasets, createDataset, deleteDataset, getDataset, genTags, genResources, updateDataset, genDescription, uploadCSV, updateResource, updateResourcesOrder, deleteResource, createResource } from './odp.js'
import { eqSet, eqResources, fetchThrottle } from './utils.js'
import dotenv from 'dotenv'
import process from 'node:process'
import { HttpsProxyAgent } from 'https-proxy-agent'
import ProxyFromEnv from 'proxy-from-env'

dotenv.config()

const changesEnabled = true
const syncCSV = false

let proxyAgent = null
if (process.env.https_proxy !== undefined) {
  proxyAgent = new HttpsProxyAgent(process.env.https_proxy)
}

// the .stat platform has 2 interesting endpoints regarding metadata: /config and /search
// the 2 following functions enable to get the data from these 2 endpoints
async function getConfig () {
  try {
    const params = {
      credentials: 'omit',
      headers: {
        Accept: 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=utf-8'
      },
      body: '{"lang":"' + process.env.dotstatLang + '","facets":{"datasourceId":["' + process.env.dotstatDatasourceId + '"]}}',
      method: 'POST'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(process.env.dotstatURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(process.env.dotstatURL + '/api/config', params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function getData (topic) {
  const params = {
    credentials: 'omit',
    headers: {
      Accept: 'application/json, text/plain, */*',
      'Content-Type': 'application/json;charset=utf-8'
    },
    body: '{"lang":"' + process.env.dotstatLang + '","search":"","facets":{"' + process.env.dotstatMainFacet + '":["' + topic + '"], "datasourceId":["' + process.env.dotstatDatasourceId + '"]},"rows":10000,"start":0, "sort": "score desc, sname asc, indexationDate desc"}',
    method: 'POST'
  }
  if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(process.env.dotstatURL)) {
    params.agent = proxyAgent
  }

  const response = await fetchThrottle(process.env.dotstatURL + '/api/search?tenant=' + process.env.tenant, params)
  if (!response.ok) {
    // some facets can have their access restricted, in this case we ignore them silently
    if (response.status === 403) {
      return { dataflows: [] }
    } else {
      response.text().then(t => { console.error(t) })
      return {}
    }
  }
  return response.json()
}

// export a dataflow as CSV
async function getCSV (id) {
  try {
    const params = {
      headers: {
        Accept: 'application/vnd.sdmx.data+csv;urn=true;file=true;labels=both',
        'Accept-Language': process.env.dotstatLang
      },
      method: 'GET'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(process.env.dotstatURL)) {
      params.agent = proxyAgent
    }

    const response = await fetchThrottle(`${process.env.dotstatURL}/rest/data/LU1,${id}/all?dimensionAtObservation=AllDimensions`, params)
    if (!response.ok) {
      response.text().then(t => { throw t })
    }
    return response.text()
  } catch (e) {
    console.error(e)
    return {}
  }
}

// convert a topic ID to an user readable topic name
function getTopicLabel (topic) {
  return topic.replace(/^\d+\|/, '').replace(/#.+?#/g, '').replace(/\|/g, ' - ')
}

// extract keywords from resources to include them on the dataset level
function getKeywordsFromResources (resources, id) {
  const keywords = resources.map(f => findKeywords(f.description)).filter(f => { return f !== undefined }).flatMap(f => { return f.split(',') }).map(f => { return f.replace(/[:;.]/g, '').trim().toLowerCase() }).map(f => { return f.replace(/[\s']/g, '-') })
  return [...new Set(keywords)]
}

function findKeywords(description) {
  let maybeKw = description.match(/[Mm]ots-cl[ée]s\s?:?\s*(.*?)\s*-/m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  maybeKw = description.match(/[Mm]ots-cl[ée]s\s?:?\s*(.*?)\s*</m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  maybeKw = description.match(/[Mm]ots-cl[ée]s\s?:?\s*(.*?)$/m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  maybeKw = description.match(/[Kk]eywords:?\s*(.*?)\s*-/m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  maybeKw = description.match(/[Kk]eywords:?\s*(.*?)\s*</m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  maybeKw = description.match(/[Kk]eywords:?\s*(.*?)$/m)
  if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
    return maybeKw[1].toLowerCase().trim()
  }
  console.log('Keywords not found in: desc:', description)
  return undefined
}

function getFrequencyFromResources (resources) {
  const mapping = {
    mensuelle: 'monthly',
    mensuel: 'monthly',
    bimestrielle: 'bimonthly',
    bimestriel: 'bimonthly',
    trimestrielle: 'quarterly',
    trimestriel: 'quarterly',
    'trois fois par an': 'threeTimesAYear',
    semestrielle: 'semiannual',
    semestriel: 'semiannual',
    'bi-annuelle': 'semiannual',
    biannuelle: 'semiannual',
    'bi-annuel': 'semiannual',
    biannuel: 'semiannual',
    annuelle: 'annual',
    anuelle: 'annual',
    annuel: 'annual',
    'révision annuelle': 'annual',
    yearly: 'annual',
    biennale: 'biennial',
    biennal: 'biennial',
    biénale: 'biennial',
    bisannuelle: 'biennial',
    bisannuel: 'biennial',
    triennale: 'triennial',
    triennal: 'triennial',
    quinquennale: 'quinquennial',
    quinquennal: 'quinquennial',
    décennale: 'decennial',
    décennal: 'decennial',
    variable: 'irregular'
  }
  // every 10 years is not supported by udata

  const order = ['unknown', 'monthly', 'bimonthly', 'quarterly', 'threeTimesAYear', 'semiannual', 'annual', 'biennial', 'triennial', 'quinquennial']

  function bestFreq (a, b) {
    if (order.indexOf(a) > order.indexOf(b)) {
      return a
    } else {
      return b
    }
  }

  let freqs = resources.map(f => {
    let maybeFreq = f.description.match(/[Pp][ée]riodicit[éey]\s?:?\s+(.*?)\s+[-\(]./)
    if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
      return maybeFreq[1].toLowerCase().trim()
    }
    maybeFreq = f.description.match(/[Pp][ée]riodicit[éey]\s?:?\s+(.*?)$/)
    if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
      return maybeFreq[1].toLowerCase().trim()
    }
    maybeFreq = f.description.match(/[Ff]r[ée]quenc[ey]\s?:?\s+(.*?)\s+-[-\(]./)
    if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
      return maybeFreq[1].toLowerCase().trim()
    }
    maybeFreq = f.description.match(/[Ff]r[ée]quenc[ey]\s?:?\s+(.*?)$/)
    if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
      return maybeFreq[1].toLowerCase().trim()
    }

    console.log('Frequency not found in: ', f.description)
    return undefined
  })
  // manage multiple frequencies (ex: annuelle & mensuelle)
  freqs = freqs.flatMap(f => {
    if (f.includes('&')) {
      return f.split('&').map(e => { return e.trim() })
    } else if (f.includes(' et ')) {
      return f.split(' et ').map(e => { return e.trim() })
    } else {
      return f
    }
  })

  freqs = freqs.filter(f => { return f !== undefined })
  freqs = [...new Set(freqs)] // remove duplicates
  freqs = freqs.map(e => {
    if (mapping[e] !== undefined) {
      // français
      return mapping[e]
    } else if (order.includes(e)) {
      // anglais
      return e
    } else {
      console.log('unknown frequency:', e)
      return 'unknown'
    }
  })
  const freq = freqs.reduce(bestFreq, 'unknown')
  return freq
}

// not all topics are mapped to a dataset. We need to filter them
function filterTopics (data) {
  function getPathLength (path) {
    return (path.split('|').length - 1)
  }

  function pathCleanup (path) {
    return path.replace(/^\d+\|/, '')
  }

  function countChildren (path, data) {
    path = pathCleanup(path)
    let count = 0
    data.forEach(e => {
      const val = pathCleanup(e.val)
      if (val !== path && val.startsWith(path)) {
        count += 1
      }
    })
    return count
  }

  let paths = data.facets[process.env.dotstatMainFacet].buckets
  paths = paths.map(e => {
    e.children = countChildren(e.val, paths)
    e.depth = getPathLength(e.val)
    return e
  })

  // we keep only topics without children
  return paths.filter(f => { return (f.children === 0) })
}

async function asCreateDataset (resources, id) {
  try {
    const keywords = getKeywordsFromResources(resources[id], id)
    const label = getTopicLabel(id)
    const ds = await createDataset(label, resources[id], id, keywords, getFrequencyFromResources(resources[id]))
    if (syncCSV) {
      await updateResourcesWithCSV(resources, id, ds)
    }

    console.log('Dataset successfully created', ds.id)
  } catch (e) {
    console.error('Error creating dataset ', e, id)
  }
}

async function asUpdateDataset (resources, id, dataset, tags) {
  try {
    const desc = await genDescription(genResources(resources[id]))
    const ds = await updateDataset(dataset.id, { description: desc, tags: [...tags], frequency: getFrequencyFromResources(resources[id]) })
    if (syncCSV) {
      await updateResourcesWithCSV(resources, id, ds)
    }

    console.log('Resources and description successfully updated for', dataset.id)
  } catch (e) {
    console.error('Error updating dataset', e, dataset.id)
  }
}

async function updateResourcesWithCSV (resources, id, ds) {
  const r = await Promise.all(resources[id].map(df => { return getCSV(df.dataflowId) }))
  const uploaded = await Promise.all(resources[id].map((df, i) => { return [df.dataflowId + '.csv', r[i]] }).map(param => { return uploadCSV(param[0], param[1], ds.id) }))
  await Promise.all(resources[id].map((df, i) => { return updateResource(ds.id, uploaded[i].id, df.name, df.description) }))
  const updatedDs = await getDataset(ds.id)
  const order = updatedDs.resources.sort((a, b) => { return (a.title !== b.title) ? (a.title.localeCompare(b.title)) : (b.format.localeCompare(a.format)) }).map(a => a.id)
  await updateResourcesOrder(ds.id, order)
}

async function main () {
  console.log((new Date()).toLocaleString(), 'Syncing starts...')

  const d = await getSyncedDatasets()
  const odpMapping = {}
  d.data.map(e => { return [e.id, e.extras.dotstat_id] }).forEach(tuple => { odpMapping[tuple[1]] = tuple[0] })
  const odpIds = new Set(d.data.map(e => { return e.extras.dotstat_id }))

  const data = await getConfig()
  const filtered = filterTopics(data).map(e => { return e.val })
  const topicsSet = new Set(filtered)
  const topicsArr = [...topicsSet]

  // get the list of items that were added, deleted, changed
  const toDelete = new Set([...odpIds].filter(x => !topicsSet.has(x)))
  const toAdd = new Set(topicsArr.filter(x => !odpIds.has(x)))
  const rest = new Set([...odpIds].filter(x => topicsSet.has(x)))

  console.log('to be deleted', [...toDelete].length)
  console.log('to be added', [...toAdd].length)
  console.log('check for updates', [...rest].length)

  // sanity check
  if ([...toAdd].length === 0 && [...rest].length === 0 && [...toDelete].length > 100) {
    throw new Error('Too many datasets to be deleted, stopping there...')
  }

  // delete what needs to be deleted
  if (changesEnabled) {
    for (const e of toDelete) {
      const id = odpMapping[e]
      const result = await deleteDataset(id)
      console.log('Dataset deletion', (result) ? 'succeeded' : 'failed', 'for', id)
    }
  }

  const r = await Promise.all(topicsArr.map(t => { return getData(t) }))
  const dataflows = r.map(d => d.dataflows)

  // get resources for each topic
  const resources = {}
  for (let i = 0; i < topicsArr.length; i++) {
    resources[topicsArr[i]] = dataflows[i]
  }

  // add what needs to be added

  if (changesEnabled) {
    for (const id of toAdd) {
      await asCreateDataset(resources, id)
    }
  }
  // check if the rest should be modified
  for (const id of rest) {
    // get matching dataset from ODP
    // console.log('Checking updates for', id)
    const dataset = await getDataset(odpMapping[id])
    // compare keywords

    // WARNING: tags should always be present in an update request, otherwise they are wiped out (bug in udata?)
    const dotstatName = getTopicLabel(id)
    const odpName = dataset.title
    const dotstatTags = new Set(genTags(dotstatName, getKeywordsFromResources(resources[id], id)))
    const odpTags = new Set(dataset.tags)

    if (!eqSet(dotstatTags, odpTags)) {
      console.log('Tags should be updated for', dataset.id)
      // console.log('old:', [...odpTags], 'new:', [...dotstatTags])
      if (changesEnabled) {
        updateDataset(dataset.id, { tags: [...dotstatTags] }).then(f => { console.log('Tags successfully updated for', dataset.id) }).catch(e => { console.error(e) })
      }
    }

    // compare name
    if (dotstatName !== odpName) {
      console.log('Title should be updated for', dataset.id)
      // console.log('old:', odpName, 'new:', dotstatName )
      if (changesEnabled) {
        updateDataset(dataset.id, { title: dotstatName, tags: [...dotstatTags] }).then(f => { console.log('Title successfully updated for:', dataset.id) }).catch(e => { console.error(e) })
      }
    }

    // compare resources
    const dotstatResources = new Set(genResources(resources[id]).map(e => { return { title: e.title, description: e.description, url: e.url } }))
    const odpResources = new Set(dataset.resources.filter(e => { return e.format === 'html' }).map(e => { return { title: e.title, description: e.description, url: e.url } }))

    if (!eqResources(dotstatResources, odpResources)) {
      console.log('Resources and description should be updated for', dataset.id)

      // prepare all data to detect what should be deleted, added, updated
      // the comparisons are done on the urls of .stat resources
      const dotstatUrls = [...dotstatResources].map(e => e.url)
      const odpUrls = [...odpResources].map(e => e.url)

      const dotstatUrlsSet = new Set([...dotstatUrls])
      const odpUrlsSet = new Set([...odpUrls])

      // mappings url => resource for .stat and udata
      const dotstatMapping = {}
      Array.from(dotstatResources).map(e => {
        dotstatMapping[e.url] = e
        return e
      })
      const odpMapping = {}
      Array.from(odpResources).map(e => {
        odpMapping[e.url] = e
        return e
      })

      // mapping url => resource ID in udata
      const odpIds = {}
      dataset.resources.filter(e => { return e.format === 'html' }).map(e => {
        odpIds[e.url] = e.id
        return e
      })

      // which resources should we delete, add, update?
      const resToDelete = new Set([...odpUrls].filter(x => !dotstatUrlsSet.has(x)))
      const resToAdd = new Set([...dotstatUrls].filter(x => !odpUrlsSet.has(x)))
      const resToUpdate = new Set([...odpUrls].filter(x => dotstatUrlsSet.has(x) && (dotstatMapping[x].title !== odpMapping[x].title || dotstatMapping[x].description !== odpMapping[x].description)))

      console.log('resToDelete', resToDelete)
      console.log('resToAdd', resToAdd)
      console.log('resToUpdate', resToUpdate)

      if (changesEnabled) {
        // delete old resources
        for (const url of resToDelete) {
          console.log('deleting resource', odpIds[url], 'from dataset', dataset.id)
          await deleteResource(dataset.id, odpIds[url])
        }

        // add new resources
        for (const url of resToAdd) {
          const res = dotstatMapping[url]
          console.log('adding resource', url, 'to the dataset', dataset.id)
          await createResource(dataset.id, res.title, res.description, res.url)
        }

        // update existing resources
        for (const url of resToUpdate) {
          const res = dotstatMapping[url]
          console.log('updating resource', odpIds[url], 'in the dataset', dataset.id)
          console.log('title', res.title, 'desc', res.description)
          await updateResource(dataset.id, odpIds[url], res.title, res.description)
        }

        // update dataset metadata (tags, description, ...)
        await asUpdateDataset(resources, id, dataset, dotstatTags)
      }
    }
  }
}

main().then(() => { console.log((new Date()).toLocaleString(), 'Sync successful') }).catch(e => { console.error('Error', e); process.exitCode = 1 })

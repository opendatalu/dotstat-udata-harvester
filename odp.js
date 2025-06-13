import ejs from 'ejs'
import keywordExtractor from 'keyword-extractor'
import dotenv from 'dotenv'
import { fetchThrottle } from './utils.js'
import { FormData, File } from 'node-fetch'
import { HttpsProxyAgent } from 'https-proxy-agent'
import ProxyFromEnv from 'proxy-from-env'

dotenv.config()

const odpURL = process.env.odpURL
const odpAPIKey = process.env.odpAPIKey
const orgId = process.env.orgId
const syncTag = process.env.syncTag
const descTemplate = './' + ((process.env.descTemplate !== undefined) ? process.env.descTemplate : 'desc.ejs')

let proxyAgent = null
if (process.env.https_proxy !== undefined) {
  proxyAgent = new HttpsProxyAgent(process.env.https_proxy)
  console.log('Proxy set to:' + process.env.https_proxy)
}

async function getSyncedDatasets () {
  try {
    // FIXME: manage pagination, temporarily a large page size here
    // console.log(odpURL+"/datasets/?tag="+syncTag+"&page=1&page_size=200&organization="+orgId)
    const params = {
      headers: {
        Accept: 'application/json, text/plain, */*',
        'X-API-KEY': odpAPIKey
      },
      method: 'GET'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(odpURL + '/datasets/?tag=' + syncTag + '&page=1&page_size=500&organization=' + orgId, params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function getAllDatasets () {
  try {
    // FIXME: manage pagination, temporarily a large page size here
    // console.log(odpURL+"/datasets/?tag="+syncTag+"&page=1&page_size=200&organization="+orgId)
    const params = {
      headers: {
        Accept: 'application/json, text/plain, */*',
        'X-API-KEY': odpAPIKey
      },
      method: 'GET'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(odpURL + '/datasets/?page=1&page_size=500&organization=' + orgId, params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

// get a language code managed by keyword-extractor
function kwExtractorLang (lang) {
  const mapping = { en: 'english', es: 'spanish', pl: 'polish', de: 'german', fr: 'french', it: 'italian', nl: 'dutch', ro: 'romanian', ru: 'russian', pt: 'portuguese', sv: 'swedish', ar: 'arabic', fa: 'persian' }
  return (mapping[lang] !== undefined) ? mapping[lang] : 'english'
}

// generate tags for a dataset
function genTags (title, keywords) {
  let tags = keywordExtractor.extract(title, {
    language: kwExtractorLang(process.env.dotstatLang),
    remove_digits: false,
    return_changed_case: true,
    remove_duplicates: true
  })
  tags = tags.concat(keywords)
  tags = tags.map(t => { return t.normalize('NFD').replace(/[\u0300-\u036f]/g, '') }) // remove accents
  tags = tags.map(t => { return t.replace(/^[a-zA-Z]+'/, '') }) // remove articles with apostrophe
  tags = tags.map(t => { return t.replace(/[.']/g, '') }) // remove special characters because udata removes them and we need to be able to compare
  tags = tags.map(t => { return t.replace(/[/()]+/g, '-') }) // udata replaces thoses characters with dashes
  tags = tags.map(t => { return t.replace(/&/g, '-and-') })
  tags = tags.map(t => { return t.replace(/--+/g, '-') }) // remove multiple dashes
  tags = [...new Set(tags)] // remove duplicates
  tags = tags.filter(t => { return (t.length >= 3 && t.length <= 96) })
  tags = tags.concat([syncTag, 'statistics', 'statistiques'])
  return tags
}

// generate resources for a dataset
function genResources (resources) {
  return resources.map(r => {
    return {
      description: r.description,
      filesize: 0,
      filetype: 'remote',
      format: 'html',
      title: r.name,
      type: 'main',
      url: process.env.dotstatDataflowURLPrefix + r.dataflowId
    }
  })
}

// generate the description of a dataset based on its resources
async function genDescription (resources) {
  const p = new Promise((resolve, reject) => {
    ejs.renderFile(descTemplate, { resources: resources.map(r => { return r.title }) }, function (err, desc) {
      if (err !== null) {
        reject(err)
      }
      resolve(desc)
    })
  })
  return p.then(d => { return d })
}

async function createDataset (title, resources, remoteId, keywords, frequency) {
  const tags = genTags(title, keywords)
  resources = genResources(resources)

  return genDescription(resources).then(desc => {
    const dataset = {
      description: desc,
      license: process.env.license,
      organization: { id: orgId },
      tags,
      title,
      frequency,
      extras: { dotstat_id: remoteId },
      spatial: {
        geom: null,
        granularity: 'country',
        zones: [
          'country:lu'
        ]
      }
    }
    if (resources.length !== 0) {
      dataset.resources = resources
    }
    // console.log(JSON.stringify(dataset))
    return createDatasetFromJSON(dataset)
  })
}

async function createDatasetFromJSON (dataset) {
  if (dataset != null) {
    try {
      const params = {
        headers: {
          Accept: 'application/json, text/plain, */*',
          'Content-Type': 'application/json;charset=utf-8',
          'X-API-KEY': odpAPIKey
        },
        body: JSON.stringify(dataset),
        method: 'POST'
      }
      if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
        params.agent = proxyAgent
      }

      const res = await fetchThrottle(odpURL + '/datasets/', params)

      if (!res.ok) {
        res.text().then(t => { throw t })
      }
      return res.json()
    } catch (e) {
      console.error(e)
      return {}
    }
  } else {
    console.error('cannot create empty dataset')
  }
}

async function deleteDataset (id) {
  try {
    const params = {
      headers: {
        Accept: 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=utf-8',
        'X-API-KEY': odpAPIKey
      },
      method: 'DELETE'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }
    const res = await fetchThrottle(odpURL + '/datasets/' + id + '/', params)

    return (res.ok)
  } catch (e) {
    console.error(e)
    return false
  }
}

async function getDataset (id) {
  try {
    const params = {
      headers: {
        Accept: 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=utf-8',
        'X-API-KEY': odpAPIKey
      },
      method: 'GET'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }
    const res = await fetchThrottle(odpURL + '/datasets/' + id + '/', params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }

    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function updateDataset (id, payload) {
  try {
    const params = {
      headers: {
        Accept: 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=utf-8',
        'X-API-KEY': odpAPIKey
      },
      body: JSON.stringify(payload),
      method: 'PUT'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(odpURL + '/datasets/' + id + '/', params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function uploadCSV (filename, data, dsId) {
  try {
    // uuid, filename, size, file*
    const formData = new FormData()
    const file = new File([data], filename, { type: 'text/csv' })

    formData.set('filename', filename)
    formData.set('file', file, filename)

    const params = {
      headers: {
        Accept: 'application/json',
        'Cache-Control': 'no-cache',
        'X-API-KEY': odpAPIKey
      },
      body: formData,
      method: 'POST'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(odpURL + '/datasets/' + dsId + '/upload/', params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function createResource (dsId, title, description, url) {
  try {
    const body = {
      description,
      filesize: 0,
      filetype: 'remote',
      format: 'html',
      title,
      type: 'main',
      url
    }

    const params = {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': odpAPIKey
      },
      body: JSON.stringify(body),
      method: 'POST'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(`${odpURL}/datasets/${dsId}/resources/`, params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function updateResource (dsId, resId, title, description) {
  try {
    const body = { title, description }

    const params = {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': odpAPIKey
      },
      body: JSON.stringify(body),
      method: 'PUT'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(`${odpURL}/datasets/${dsId}/resources/${resId}/`, params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function deleteResource (dsId, resId) {
  try {
    const params = {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': odpAPIKey
      },
      method: 'DELETE'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(`${odpURL}/datasets/${dsId}/resources/${resId}/`, params)
    return res.ok
  } catch (e) {
    console.error(e)
    return {}
  }
}

async function updateResourcesOrder (dsId, order) {
  try {
    const params = {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': odpAPIKey
      },
      body: JSON.stringify(order),
      method: 'PUT'
    }
    if (proxyAgent !== null && ProxyFromEnv.getProxyForUrl(odpURL)) {
      params.agent = proxyAgent
    }

    const res = await fetchThrottle(`${odpURL}/datasets/${dsId}/resources/`, params)
    if (!res.ok) {
      res.text().then(t => { throw t })
    }
    return res.json()
  } catch (e) {
    console.error(e)
    return {}
  }
}

export { getAllDatasets, getSyncedDatasets, getDataset, createDataset, deleteDataset, genTags, genResources, updateDataset, genDescription, uploadCSV, createResource, updateResource, updateResourcesOrder, deleteResource }

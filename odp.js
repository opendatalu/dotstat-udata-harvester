import ejs from 'ejs'
import keyword_extractor from 'keyword-extractor'
import dotenv from 'dotenv'
import { fetchThrottle } from './utils.js'
import { FormData, File, fileFromSync  } from 'node-fetch'

dotenv.config()

const odpURL = process.env.odpURL
const odpAPIKey = process.env.odpAPIKey
const orgId = process.env.orgId
const syncTag = process.env.syncTag
const descTemplate = './'+((process.env.descTemplate !== undefined)?process.env.descTemplate:'desc.ejs')

async function getSyncedDatasets() {
    try {
        // FIXME: manage pagination, temporarily a large page size here
        const res = await fetchThrottle(odpURL+"/datasets/?tag="+syncTag+"&page=0&page_size=200&organization="+orgId, {
        "headers": {
            "Accept": "application/json, text/plain, */*",
            'X-API-KEY': odpAPIKey
        },
        "method": "GET"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }
        return res.json()
    } catch(e) {
        console.error(e)
        return {}
    }    
}

// get a language code managed by keyword-extractor
function kwExtractorLang(lang) {
    const mapping = { 'en': 'english', 'es': 'spanish', 'pl': 'polish', 'de': 'german', 'fr': 'french', 'it': 'italian', 'nl': 'dutch', 'ro': 'romanian', 'ru': 'russian', 'pt': 'portuguese', 'sv': 'swedish', 'ar': 'arabic', 'fa': 'persian'} 
    return (mapping[lang] !== undefined)?mapping[lang]:'english'
}

// generate tags for a dataset
function genTags(title, keywords) {
    let tags = keyword_extractor.extract(title, {
        language: kwExtractorLang(process.env.dotstatLang), 
        remove_digits: false,
        return_changed_case: true, 
        remove_duplicates: true
    })
    tags = tags.concat(keywords)
    tags = tags.map(t => { return t.normalize("NFD").replace(/[\u0300-\u036f]/g, "") }) // remove accents
    tags = tags.map(t => { return t.replace(/^[a-zA-Z]+\'/, '')}) // remove articles with apostrophe
    tags = tags.map(t => { return t.replace(/[\.\']/g, '')}) // remove special characters because udata removes them and we need to be able to compare
    tags = [... new Set(tags)] // remove duplicates
    tags = tags.filter(t => { return (t.length >= 3)})
    tags = tags.concat([ syncTag, 'statistics', 'statistiques'])
    return tags
}

// generate resources for a dataset
function genResources(resources) {
    return resources.map(r => {return {
        "description": r.description,
        "filesize": 0,
        "filetype": "remote",
        "format": "html",
        "title": r.name,
        "type": "main",
        "url": process.env.dotstatDataflowURLPrefix+r.dataflowId
      }
    })
}

// generate the description of a dataset based on its resources
async function genDescription(resources) {
    const p = new Promise((resolve,reject) => {
        ejs.renderFile(descTemplate, {resources: resources.map(r => {return r.title}) }, function(err, desc){
            if (err !== null) {
                reject(err)
            }
            resolve(desc)
        })
    })
    return p.then(d => {return d})    
}

async function createDataset(title, resources, remote_id, keywords, frequency) {
    const tags = genTags(title, keywords)
    resources = genResources(resources)

    return genDescription(resources).then(desc => {
        const dataset = {
            "description": desc, 
            "frequency": "unknown", 
            "license": process.env.license, 
            "organization": {"id": orgId }, 
            "tags": tags,
            "title": title,
            "frequency": frequency,
            "extras": { "harvest:domain" : process.env.dotstatURL, "harvest:remote_id": remote_id }
        }
        if (resources.length != 0) {
            dataset.resources = resources
        }
        return createDatasetFromJSON(dataset)
    })
}

async function createDatasetFromJSON(dataset) {
    if (dataset != null) {
        try {
            const res = await fetchThrottle(odpURL+"/datasets/", {
            "headers": {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=utf-8",
                'X-API-KEY': odpAPIKey
            },
            "body": JSON.stringify(dataset),
            "method": "POST"
            })

            if (!res.ok) {
                res.text().then(t => { throw t})
            }
            return res.json()
        } catch(e) {
            console.error(e)
            return {}
        }
    } else {
        console.error('cannot create empty dataset')
    }
}

async function deleteDataset(id) {
    try {
        const res = await fetchThrottle(odpURL+"/datasets/"+id+"/", {
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8",
            'X-API-KEY': odpAPIKey
        },
        "method": "DELETE"
        })
        
        return (res.ok)
    } catch(e) {
        console.error(e)
        return false
    }    
}


async function getDataset(id) {
    try {
        const res = await fetchThrottle(odpURL+"/datasets/"+id+"/", {
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8",
            'X-API-KEY': odpAPIKey
        },
        "method": "GET"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }

        return res.json()
    } catch(e) {
        console.error(e)
        return {}
    }    
}

async function updateDataset(id, payload) {
    try {
        const res = await fetchThrottle(odpURL+"/datasets/"+id+"/", {
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8",
            'X-API-KEY': odpAPIKey
        },
        "body": JSON.stringify(payload),
        "method": "PUT"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }
        return res.json()
    } catch(e) {
        console.error(e)
        return {}
    }    
}


async function uploadCSV(filename, data, ds_id) {
    try {
        // uuid, filename, size, file*
        const formData = new FormData()
        const file = new File([data], filename, {'type': 'text/csv'})

        formData.set('filename', filename)
        formData.set('file', file, filename)

        const res = await fetchThrottle(odpURL+'/datasets/'+ds_id+'/upload/', {
        "headers": {
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            'X-API-KEY': odpAPIKey
        },
        "body": formData,
        "method": "POST"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }
        return res.json()
    } catch (e) {
        console.error(e)
        return {}
    }

}

async function updateResource(ds_id, res_id, title, desc) {
    try {
        const body = {'title': title, 'description': desc}
        const res = await fetchThrottle(`${odpURL}/datasets/${ds_id}/resources/${res_id}/`, {
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/json",
                'X-API-KEY': odpAPIKey
            },
            "body": JSON.stringify(body),
            "method": "PUT"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }
        return res.json()        
    } catch (e) {
        console.error(e)
        return {}
    }
}

async function deleteResource(ds_id, res_id) {
    try {
        const res = await fetchThrottle(`${odpURL}/datasets/${ds_id}/resources/${res_id}/`, {
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/json",
                'X-API-KEY': odpAPIKey
            },
            "method": "DELETE"
        })
        return res.ok        
    } catch (e) {
        console.error(e)
        return {}
    }    
}

async function updateResourcesOrder(ds_id, order) {
    try {
        const res = await fetchThrottle(`${odpURL}/datasets/${ds_id}/resources/`, {
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/json",
                'X-API-KEY': odpAPIKey
            },
            "body": JSON.stringify(order),
            "method": "PUT"
        })
        if (!res.ok) {
            res.text().then(t => { throw t})
        }
        return res.json()        
    } catch (e) {
        console.error(e)
        return {}
    }    
}


export { getSyncedDatasets, getDataset, createDataset, deleteDataset, genTags, genResources, updateDataset, genDescription, uploadCSV, updateResource, updateResourcesOrder, deleteResource }



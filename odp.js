import ejs from 'ejs'
import keyword_extractor from 'keyword-extractor'
import dotenv from 'dotenv'
import { fetchThrottle } from './utils.js'

dotenv.config()

const odpURL = process.env.odpURL
const odpAPIKey = process.env.odpAPIKey
const statecOrgId = process.env.statecOrgId
const statecTag = 'statec-sync'

async function getSyncedDatasets() {
    try {
        // FIXME: manage pagination, temporarily a large page size here
        const res = await fetchThrottle(odpURL+"/datasets/?tag="+statecTag+"&page=0&page_size=200", {
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

function genTags(title, keywords) {
    let tags = keyword_extractor.extract(title, {
        language: 'french', 
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
    tags = tags.concat([ statecTag, 'statistics', 'statistiques'])
    return tags
}

function genResources(resources) {
    return resources.map(r => {return {
        "description": r.description,
        "filesize": 0,
        "filetype": "remote",
        "format": "html",
        "title": r.name,
        "type": "other",
        "url": "https://lustat.statec.lu/vis?df%5Bds%5D=release&df%5Bid%5D="+r.dataflowId+"&df%5Bag%5D=LU1"
      }
    })
}

async function genDescription(resources) {
    const p = new Promise((resolve,reject) => {
        ejs.renderFile('./desc.ejs', {resources: resources.map(r => {return r.title}) }, function(err, desc){
            if (err !== null) {
                reject(err)
            }
            resolve(desc)
        })
    })
    return p.then(d => {return d})    
}

async function createDataset(title, resources, remote_id, keywords =[]) {
    const tags = genTags(title, keywords)
    resources = genResources(resources)

    return genDescription(resources).then(desc => {
        const dataset = {
            "description": desc, 
            "frequency": "unknown", 
            "license": 'cc-zero', 
            "organization": {"id": statecOrgId }, 
            "tags": tags,
            "title": title,
            "extras": { "harvest:domain" : "lustat.statec.lu", "harvest:remote_id": remote_id }
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

export { getSyncedDatasets, getDataset, createDataset, deleteDataset, genTags, genResources, updateDataset, genDescription }



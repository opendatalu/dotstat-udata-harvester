import { getSyncedDatasets, createDataset, deleteDataset, getDataset, genTags, genResources, updateDataset, genDescription, uploadCSV, updateResource, updateResourcesOrder, deleteResource} from './odp.js'
import { eqSet, eqResources, fetchThrottle } from './utils.js'
import dotenv from 'dotenv'
import process from 'node:process';

dotenv.config()

const changesEnabled = true
const syncCSV = false

// the .stat platform has 2 interesting endpoints regarding metadata: /config and /search
// the 2 following functions enable to get the data from these 2 endpoints
async function getConfig() {
    try {
        const res = await fetchThrottle(process.env.dotstatURL+"/api/config", {
        "credentials": "omit",
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8"
        },
        "body": "{\"lang\":\""+process.env.dotstatLang+"\",\"facets\":{\"datasourceId\":[\""+process.env.dotstatDatasourceId+"\"]}}",
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
}

async function getData(topic) {
    try {
        const response = await fetchThrottle(process.env.dotstatURL+"/api/search", {
            "credentials": "omit",
            "headers": {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=utf-8"
            },
            "body": "{\"lang\":\""+process.env.dotstatLang+"\",\"search\":\"\",\"facets\":{\""+process.env.dotstatMainFacet+"\":[\""+topic+"\"], \"datasourceId\":[\""+process.env.dotstatDatasourceId+"\"]},\"rows\":10000,\"start\":0}",
            "method": "POST"
        })
        if (!response.ok) {
            response.text().then(t => { throw t})
        }
        return response.json()
    } catch(e) {
        console.error(e)
        return {}
    }
}

// export a dataflow as CSV 
async function getCSV(id) {
    try {
        const response = await fetchThrottle(`${process.env.dotstatURL}/rest/data/LU1,${id}/all?dimensionAtObservation=AllDimensions`, {
            "headers": {
                "Accept": "application/vnd.sdmx.data+csv;urn=true;file=true;labels=both",
                "Accept-Language": process.env.dotstatLang
            },
            "method": "GET"
        })
        if (!response.ok) {
            response.text().then(t => { throw t})
        }
        return response.text()        
    } catch(e) {
        console.error(e)
        return {}
    }
}

// convert a topic ID to an user readable topic name
function getTopicLabel(topic) {
    return topic.replace(/^\d+\|/, '').replace(/#.+?#/g,'').replace(/\|/g, ' - ')
}

// extract keywords from resources to include them on the dataset level 
function getKeywordsFromResources(resources, id) {
    const keywords = resources.map(f => { 
        let maybeKw = f.description.match(/[Mm]ots-cl[ée]s\s?:?\s*(.*?)\s*-/)
        if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
                return maybeKw[1].toLowerCase().trim()
        }
        maybeKw = f.description.match(/[Mm]ots-cl[ée]s\s?:?\s*(.*?)$/)
        if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
                return maybeKw[1].toLowerCase().trim()
        }
        maybeKw = f.description.match(/[Kk]eywords:?\s*(.*?)\s+-/)
        if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
                return maybeKw[1].toLowerCase().trim()
        }
        maybeKw = f.description.match(/[Kk]eywords:?\s*(.*?)$/)
        if ((maybeKw !== null) && (maybeKw[1] !== undefined)) {
                return maybeKw[1].toLowerCase().trim()
        }                
        console.log('Keywords not found in: ', id, 'desc:', f.description)
        return undefined
    }).filter(f=> {return f!=undefined}).flatMap(f=> {return f.split(',')}).map(f=> {return f.replace(/[:;\.]/g, '').trim().toLowerCase()}).map(f=>{return f.replace(/[\s\']/g, '-')})
    return [...new Set(keywords)]
}

function getFrequencyFromResources(resources) {
    // FIXME: check if lang == fr
    const mapping = {
        "mensuelle":"monthly",
        "mensuel":"monthly", 
        "bimestrielle":"bimonthly", 
        "bimestriel":"bimonthly",
        "trimestrielle":"quarterly", 
        "trimestriel":"quarterly",
        "trois fois par an":"threeTimesAYear", 
        "semestrielle":"semiannual",
        "semestriel":"semiannual",
        "bi-annuelle":"semiannual",
        "biannuelle":"semiannual",
        "bi-annuel":"semiannual",
        "biannuel":"semiannual",
        "annuelle": "annual",
        "annuel": "annual",
        "révision annuelle": "annual",
        "semestrielle et annuelle": "annual",
        "biennale":"biennial",
        "biennal":"biennial",
        "biénale": "biennial",
        "bisannuelle":"biennial",
        "bisannuel":"biennial",
        "triennale": "triennial",
        "triennal": "triennial", 
        "quinquennale" : "quinquennial",
        "quinquennal" : "quinquennial",
        "variable": "irregular"
    }
    // every 10 years is not supported by udata

    const order = ['unknown', 'monthly', 'bimonthly','quarterly', 'threeTimesAYear', 'semiannual', 'annual', 'biennial', 'triennial', 'quinquennial']

    function bestFreq(a, b) {
        if (order.indexOf(a) > order.indexOf(b)) {
            return a
        } else {
            return b
        }
    }

    let freqs = resources.map(f => {
        let maybeFreq = f.description.match(/[Pp][ée]riodicit[ée]\s?:?\s+(.*?)\s+-/)
        if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
                return maybeFreq[1].toLowerCase().trim()
        }
        maybeFreq = f.description.match(/[Pp][ée]riodicit[ée]\s?:?\s+(.*?)$/)
        if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
                return maybeFreq[1].toLowerCase().trim()
        }
        maybeFreq = f.description.match(/[Ff]r[ée]quence\s?:?\s+(.*?)\s+-/)
        if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
                return maybeFreq[1].toLowerCase().trim()
        }
        maybeFreq = f.description.match(/[Ff]r[ée]quence\s?:?\s+(.*?)$/)
        if ((maybeFreq !== null) && (maybeFreq[1] !== undefined)) {
                return maybeFreq[1].toLowerCase().trim()
        }
        console.log('Frequency not found in: ', f.description)
        return undefined
    })
    freqs = freqs.filter(f=> {return f!=undefined})
    freqs = [...new Set(freqs)] // remove duplicates
    freqs = freqs.map(e => { 
        if (mapping[e] !== undefined) {
            return mapping[e] 
        } else {
            console.log('unknown frequency:', e)
            return 'unknown'
        }
    })
    const freq = freqs.reduce(bestFreq, 'unknown')
    return freqs
}

// not all topics are mapped to a dataset. We need to filter them
function filterTopics(data) {
    function getPathLength(path) {
        return (path.split('|').length -1)
    }

    function pathCleanup(path) {
        return path.replace(/^\d+\|/, '')
    }

    function countChildren(path, data) {
        path = pathCleanup(path)
        let count = 0
        data.forEach(e => {
            const val = pathCleanup(e.val)
            if (val != path && val.startsWith(path)) {
                count += 1
            }
        })
        return count
    }

    let paths = data['facets'][process.env.dotstatMainFacet]['buckets']
    paths = paths.map(e => {
        e.children = countChildren(e.val, paths)
        e.depth = getPathLength(e.val)
        return e
    })

    // we keep only topics without children
    return paths.filter(f => {return (f.children == 0)})
}

async function asCreateDataset(resources, id) {
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

async function asUpdateDataset(resources, id, dataset, tags) {
    try {
        const desc = await genDescription(genResources(resources[id]))
        await Promise.all(dataset.resources.map(res => {return deleteResource(dataset.id, res.id)}))
        const ds = await updateDataset(dataset.id, { 'resources': genResources(resources[id]), 'description': desc, 'tags': [...tags], 'frequency': getFrequencyFromResources(resources[id]) })
        if (syncCSV) {
            await updateResourcesWithCSV(resources, id, ds)
        }
            
        console.log('Resources and description successfully updated for', dataset.id)
    } catch (e) {
        console.error('Error updating dataset', e, dataset.id)
    }
}

async function updateResourcesWithCSV(resources, id, ds) {
    const r = await Promise.all(resources[id].map(df => { return getCSV(df['dataflowId']) }))
    const uploaded = await Promise.all(resources[id].map((df, i) => { return [df.dataflowId+'.csv', r[i]] }).map(param => { return uploadCSV(param[0], param[1], ds.id)}))
    await Promise.all(resources[id].map((df, i) => { return updateResource(ds.id, uploaded[i].id, df.name, df.description)} ))
    const updated_ds = await getDataset(ds.id)
    const order = updated_ds.resources.sort((a,b) => { return (a.title != b.title)?(a.title.localeCompare(b.title)):(b.format.localeCompare(a.format)) }).map(a => a.id)
    await updateResourcesOrder(ds.id, order)
}

async function main() {
    console.log((new Date()).toLocaleString(), 'Syncing starts...')

    const d = await getSyncedDatasets()
    let odpMapping = {}
    d.data.map(e => {return [e.id, e.extras['dotstat_id']]}).forEach(tuple => { odpMapping[tuple[1]] = tuple[0] } )
    const odpIds = new Set(d.data.map(e => {return  e.extras['dotstat_id']}))

    const data = await getConfig()
    const filtered = filterTopics(data).map(e => {return e.val})
    const topicsSet = new Set(filtered)
    const topicsArr = [...topicsSet]

    // get the list of items that were added, deleted, changed
    const toDelete = new Set([...odpIds].filter(x => !topicsSet.has(x)))
    const toAdd = new Set(topicsArr.filter(x => !odpIds.has(x)))
    const rest = new Set([...odpIds].filter(x => topicsSet.has(x)))

    console.log('to be deleted', [...toDelete].length)
    console.log('to be added', [...toAdd].length)
    console.log('check for updates', [...rest].length)

    // delete what needs to be deleted
    if (changesEnabled) {
        for (const e of toDelete) {
            const id = odpMapping[e]
            const result = await deleteDataset(id)
            console.log('Dataset deletion', (result)?'succeeded': 'failed', 'for', id)
        }
    }

    const r = await Promise.all(topicsArr.map(t => {return getData(t)}))
    const dataflows = r.map(d => d.dataflows)

    // get resources for each topic
    let resources = {}
    for (let i=0; i<topicsArr.length; i++) {
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
        //console.log('Checking updates for', id)
        const dataset = await getDataset(odpMapping[id])
        // compare keywords

        // WARNING: tags should always be present in an update request, otherwise they are wiped out (bug in udata?)
        const dotstatName = getTopicLabel(id)
        const odpName = dataset.title
        const dotstatTags = new Set(genTags(dotstatName, getKeywordsFromResources(resources[id], id)))
        const odpTags = new Set(dataset.tags)

        if (!eqSet(dotstatTags, odpTags)) {
            console.log('Tags should be updated for', dataset.id)
            //console.log('old:', [...odpTags], 'new:', [...dotstatTags])
            if (changesEnabled){
                updateDataset(dataset.id, {'tags': [...dotstatTags]}).then(f => {console.log('Tags successfully updated for', dataset.id)}).catch(e=>{console.error(e)})
            }
        }

        // compare name
        if (dotstatName != odpName) {
            console.log('Title should be updated for', dataset.id)
            //console.log('old:', odpName, 'new:', dotstatName )
            if (changesEnabled) {
                updateDataset(dataset.id, {'title': dotstatName, 'tags': [...dotstatTags]}).then(f => {console.log('Title successfully updated for:', dataset.id)}).catch(e=>{console.error(e)})
            }
        } 

        // compare resources
        const dotstatResources = new Set(genResources(resources[id]).map(e => {return {"title": e.title, "description": e.description, "url": e.url}}))
        const odpResources = new Set(dataset.resources.filter(e => {return e.format == 'html'}).map(e => {return {"title": e.title, "description": e.description, "url": e.url}}))
        if (!eqResources(dotstatResources, odpResources)) {
            console.log('Resources and decription should be updated for', dataset.id)
            //console.log('old:', odpResources, 'new:', dotstatResources)
            // update resources + desc
            if (changesEnabled) {
                await asUpdateDataset(resources, id, dataset, dotstatTags)
            }

        } 
    }
}

main().then(() => {console.log((new Date()).toLocaleString(), 'Sync successful')}).catch(e => {console.error(e); process.exitCode = 1;})

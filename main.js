import { getSyncedDatasets, createDataset, deleteDataset, getDataset, genTags, genResources, updateDataset, genDescription} from './odp.js'
import { eqSet, eqResources, fetchThrottle } from './utils.js'
import dotenv from 'dotenv'

dotenv.config()

const changesEnabled = true

// the dotstat platform has 2 interesting endpoints regarding metadata: /config and /search
// the 2 following functions enable to get the data from these 2 endpoints
async function getConfig() {
    try {
        const res = await fetchThrottle(process.env.lustatURL+"/config", {
        "credentials": "omit",
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8"
        },
        "body": "{\"lang\":\"fr\",\"facets\":{\"datasourceId\":[\"release\"]}}",
        "method": "POST",
        "mode": "cors"
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
        const response = await fetchThrottle(process.env.lustatURL+"/search", {
            "credentials": "omit",
            "headers": {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=utf-8"
            },
            "body": "{\"lang\":\"fr\",\"search\":\"\",\"facets\":{\"Thèmes\":[\""+topic+"\"], \"datasourceId\":[\"release\"]},\"rows\":10000,\"start\":0}",
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

// convert a topic ID to an user readable topic name
function getTopicLabel(topic) {
    return topic.replace(/^\d+\|/, '').replace(/#.+?#/g,'').replace(/\|/g, ' - ')
}

// extract keywords from resources to include them on the dataset level 
function getKeywordsFromResources(resources) {
    const keywords = resources.map(f => {return f.description.split("Mots-clés:")[1]}).filter(f=> {return f!=undefined}).flatMap(f=> {return f.split(',')}).map(f=> {return f.trim().toLowerCase()}).map(f=>{return f.replace(/[\s\']/g, '-')})
    return [...new Set(keywords)]
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

    let paths = data['facets']['Thèmes']['buckets']
    paths = paths.map(e => {
        e.children = countChildren(e.val, paths)
        e.depth = getPathLength(e.val)
        return e
    })

    // we keep only topics without children
    return paths.filter(f => {return (f.children == 0)})
    //return paths.filter(f => {return ((f.depth <3 && f.children == 0) || (f.depth == 3))})
}


getSyncedDatasets().then(d => {
    let odpMapping = {}
    d.data.map(e => {return [e.id, e.extras['harvest:remote_id']]}).forEach(tuple => { odpMapping[tuple[1]] = tuple[0] } )
    const odpIds = new Set(d.data.map(e => {return  e.extras['harvest:remote_id']}))

    getConfig().then(data => {
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
            toDelete.forEach(e => {
               deleteDataset(odpMapping[e]).then(a => { console.log('Dataset deletion', (a)?'succeeded': 'failed', 'for', e)})
            })
        }

        Promise.all(topicsArr.map(t => {return getData(t)})).then(r => { 
            const dataflows = r.map(d => d.dataflows)

            // get resources for each topic
            let resources = {}
            for (let i=0; i<topicsArr.length; i++) {
                resources[topicsArr[i]] = dataflows[i]
            }

            // add what needs to be added

            if (changesEnabled) {
                toAdd.forEach(id => {
                    let keywords = getKeywordsFromResources(resources[id])
                    const label = getTopicLabel(id)
                    createDataset(label, resources[id], id, keywords).then(e => {console.log('Dataset successfully added for', id)}).catch(e=> console.error(e))
                })
            }
            // check if the rest should be modified
            rest.forEach(id => {
                // get matching dataset from ODP
                console.log('Checking updates for', id)
                getDataset(odpMapping[id]).then(dataset => {
                    // compare keywords

                    // WARNING: tags should always be present in an update request, otherwise they are wiped out (bug in udata?)
                    const statecName = getTopicLabel(id)
                    const odpName = dataset.title
                    const statecTags = new Set(genTags(statecName, getKeywordsFromResources(resources[id])))
                    const odpTags = new Set(dataset.tags)

                    if (!eqSet(statecTags, odpTags)) {
                        console.log('Tags should be updated for', dataset.id)
                        //console.log('old:', [...odpTags], 'new:', [...statecTags])
                        if (changesEnabled){
                            updateDataset(dataset.id, {'tags': [...statecTags]}).then(f => {console.log('Tags successfully updated for', dataset.id)}).catch(e=>{console.error(e)})
                        }
                    }

                    // compare name
                    if (statecName != odpName) {
                        console.log('Title should be updated for', dataset.id)
                        //console.log('old:', odpName, 'new:', statecName )
                        if (changesEnabled) {
                            updateDataset(dataset.id, {'title': statecName, 'tags': [...statecTags]}).then(f => {console.log('Title successfully updated for:', dataset.id)}).catch(e=>{console.error(e)})
                        }
                    } 

                    // compare resources
                    const statecResources = new Set(genResources(resources[id]).map(e => {return {"title": e.title, "description": e.description, "url": e.url}}))
                    const odpResources = new Set(dataset.resources.map(e => {return {"title": e.title, "description": e.description, "url": e.url}}))
                    if (!eqResources(statecResources, odpResources)) {
                        console.log('Resources and decription should be updated for', dataset.id)
                        //console.log('old:', odpResources, 'new:', statecResources)
                        // update resources + desc
                        if (changesEnabled) {
                            genDescription(genResources(resources[id])).then(desc => {
                                updateDataset(dataset.id, {'resources': genResources(resources[id]), 'description': desc, 'tags': [...statecTags] }).then(f => {console.log('Resources and description successfully updated for', dataset.id)}).catch(e=>{console.error(e)})
                            })
                        }

                    } 
                }).catch(e => console.error(e))
            })
        }).catch(e => { console.error(e) })
    }).catch(e => {console.error(e)})
}).catch(e => {console.error(e)})


import { getSyncedDatasets,  deleteDataset } from './odp.js'
import dotenv from 'dotenv'

dotenv.config()

getSyncedDatasets().then(d => {
    const ids = d.data.map(e => {return e.id})
    ids.forEach(e => {
        deleteDataset(e).then(a => { console.log('Dataset deletion', (a)?'succeeded': 'failed', 'for', e)})
    });
})
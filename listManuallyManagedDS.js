import { getAllDatasets } from './odp.js'
import dotenv from 'dotenv'

dotenv.config()

console.log('Title;URL')
getAllDatasets().then(d => {
  d.data.filter(e => { return !e.tags.includes('statec-sync') }).map(e => console.log(e.title + ';' + e.page))
})

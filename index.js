const Pg = require('pg');
const QueryBuilder = require('./QueryBuilder');
const Reflector = require('./Reflector');

module.exports = class Postgres {

    // TODO: convert
    /* *****
    rollback( e ) {
        return this.P( this.client.query, [ 'ROLLBACK' ], this.client )
        .then( () => { this.done(); return Promise.reject(e) } )
        .catch( error => { console.log(`Error rolling back: ${error}, ${e}`); return Promise.reject(e) } )
    },

    stream( query, pipe ) {
        return this.connect().then( () =>
            new Promise( ( resolve, reject ) => {
                const stream = this.client.query( this.CopyTo( query ) )
                stream.pipe( pipe )
                stream.on( 'end', () => { resolve(); this.done() } )
                stream.on( 'error', e => { reject(e); this.done() } )
            } )
        )
    },

    transaction( queries ) {
        return this.connect().then( () => {
            let chain = this.P( this.client.query, [ `BEGIN` ], this.client ).catch( e => this.rollback(e) )

            queries.forEach( query => chain = chain.then( () => this.P( this.client.query, query, this.client ).catch( e => this.rollback( e ) ) ) )

            return chain.then( () => this.P( this.client.query, [ 'COMMIT' ], this.client ).then( () => Promise.resolve( this.done() ) ) )
        } )
    }
    CopyTo: require('pg-copy-streams').to,
    ***** */

    constructor(args={}) {
        this.pool = new Pg.Pool({...args})

        this.pool.on('error', (err, client) => {
            console.log(`${new Date()} -- Unexpected error on idle client -- ${err.stack || err}`);
            //process.exit(-1)
        });
    }

    async insert( name, data, opts={} ) {
        const keys = Object.keys( data ),
            columns = keys.map( QueryBuilder.wrap ).join(', '),
            table = this.resources[ name ],
            nullColumns = table.columns.filter( column => !columns.includes( column.name ) ).map( column => column.name ),
            nullColumnsStr = nullColumns.length ? `, ${nullColumns.map( QueryBuilder.wrap ).join(', ')}` : '',
            nullVals = nullColumns.length ? `, ${nullColumns.map( column => `NULL` ).join(', ')}` : '',
            queryData = QueryBuilder.getVarsValues( this.resources[name].model, data, keys )
        
        let upsert = ``,
            upsertVals = [ ]
            
        if( opts.upsert ) {
            const upsertKeys = Object.keys( opts.upsert ),
                whereClause = `WHERE ${QueryBuilder.columnToVar( upsertKeys, { alias: name, baseIndex: queryData.vals.length + 1, join: ' AND ' } )}`

            upsert = `ON CONFLICT ( ${upsertKeys.map( key => `"${key}"` ).join(', ')} ) DO UPDATE SET ( ${columns}${nullColumnsStr} ) = ( ${queryData.vars.join(', ')}${nullVals} ) ${whereClause} `
            upsertVals = upsertKeys.map( key => opts.upsert[ key ] )
        }

        return this.query( `INSERT INTO ${name} ( ${columns} ) VALUES ( ${ queryData.vars.join(', ') } ) ${upsert} RETURNING ${QueryBuilder.getSimpleSelect(table.columns)}`, queryData.vals.concat( upsertVals ) )
    }

    async reflect() {
        const reflection = await Reflector.reflect(this.query.bind(this));
        Object.assign( this, reflection );
    }

    async query(query, args, opts = { }) {
        const client = await this.pool.connect()
        let result = []
        try {
            result = (await client.query( query, args )).rows
        } catch(e) {
            console.log(`${new Date()} -- Error running query: ${query} -- args ${args}`);
            throw new Error(e);
        } finally {
            client.release()
        }
        return result;
    }
}

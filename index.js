const Pg = require('pg');
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

    constructor(args=undefined) {
        this.pool = new Pg.Pool(args)

        this.pool.on('error', (err, client) => {
            console.log(`${new Date()} -- Unexpected error on idle client -- ${err.stack || err}`);
            //process.exit(-1)
        });
    }

    async reflect() {
        const reflection = await Reflector(this.query.bind(this));
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

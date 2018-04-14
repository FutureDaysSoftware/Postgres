module.exports = class QueryBuilder {

    static columnToVar( columns, opts={} ) {
        const baseIndex = opts.baseIndex || 1,
            join = opts.join || ', ',
            alias = opts.alias ? `"${opts.alias}".` : ``
        return columns.length
            ? columns.map( ( key, i ) => `${alias}"${key}" = $${ i + baseIndex }` ).join( join ) 
            : ''
    }
   
    static getSelect( { columns, table, coalesce, columnOnly } ) {
        return columns.map( column => {
            let str = `"${table}"."${column}"`;
            if(coalesce) {
                str = `COALESCE(${str},'[Null]')`
            }
            const as = columnOnly 
                ? `AS "${column}"`
                : `AS "${table}.${column}"`
            return `${str} ${as}`
        } )
        .join(', ')
    }

    static getValue( column, data ) {
        //TODO: validate?
        return column.range === 'Geography'
            ? [ data[0], data[1] ]
            : data
    }

    static getVar( column, data, index ) {
        const vars = column.range === 'Geography'
            ? `ST_Makepoint( $${index++}, $${index++} )`
            : column.range === 'Array' || ( column.isEnum && Array.isArray( column.range ) )
                ? `ARRAY[ ` + data.map( datum => `$${index++}` ).join(', ') + ` ]`
                : `$${index++}`
        return { vars, index }
    }

    static getVarsValues( table, data, keys, opts={} ) {
        const tableModel = this.tables[ table ].model
        let index = opts.baseIndex || 1

        return keys.reduce( ( memo, key ) => {
            const column = tableModel.store.name[ key ],
                datum = data[ key ],
                varResult = this.getVar( column, datum, index )

            index = varResult.index
            memo.vars = memo.vars.concat( varResult.vars )
            memo.vals = memo.vals.concat( this.getValue( column, datum ) )

            return memo
        }, { vars: [ ], vals: [ ] } )
    }
     
    static truncate( tables ) {
        return `TRUNCATE ` + tables.map( table => `"${table}"` ).join(', ');
    }

    static wrap( something ) { return `"${something}"` }

    getSelectList( table, opts={} ) {
        return typeof table === 'string'
            ? this._getSelect( table, opts.alias ? opts.alias : table )
            : table.map( t => this._getSelect( t, opts.alias ? opts.alias[ t ] : t ) ).join(', ')
    }

    _getSelect( table, alias ) {
        return this.tables[ table ].columns.map( column => `"${alias}"."${column.name}" as "${alias}.${column.name}"` ).join(', ')
    }

    columnToVar( columns, opts={} ) {
        const baseIndex = opts.baseIndex || 1,
            join = opts.join || ', ',
            alias = opts.alias ? `"${opts.alias}".` : ``
        return columns.length
            ? columns.map( ( key, i ) => `${alias}"${key}" = $${ i + baseIndex }` ).join( join ) 
            : ''
    }

    getValue( column, data ) {
        //TODO: validate?
        return column.range === 'Geography'
            ? [ data[0], data[1] ]
            : data
    }

    getVar( column, data, index ) {
        const vars = column.range === 'Geography'
            ? `ST_Makepoint( $${index++}, $${index++} )`
            : column.range === 'Array' || ( column.isEnum && Array.isArray( column.range ) )
                ? `ARRAY[ ` + data.map( datum => `$${index++}` ).join(', ') + ` ]`
                : `$${index++}`
        return { vars, index }
    }

    getVarsValues( table, data, keys, opts={} ) {
        const tableModel = this.tables[ table ].model
        let index = opts.baseIndex || 1

        return keys.reduce( ( memo, key ) => {
            const column = tableModel.store.name[ key ],
                datum = data[ key ],
                varResult = this.getVar( column, datum, index )

            index = varResult.index
            memo.vars = memo.vars.concat( varResult.vars )
            memo.vals = memo.vals.concat( this.getValue( column, datum ) )

            return memo
        }, { vars: [ ], vals: [ ] } )
    }

    wrap( something ) { return `"${something}"` }

    insert( name, data, opts={} ) {
        const keys = Object.keys( data ),
            columns = keys.map( this.wrap ).join(', '),
            nullColumns = this.tables[ name ].columns.filter( column => !columns.includes( column.name ) ).map( column => column.name ),
            nullColumnsStr = nullColumns.length ? `, ${nullColumns.map( this.wrap ).join(', ')}` : '',
            nullVals = nullColumns.length ? `, ${nullColumns.map( column => `NULL` ).join(', ')}` : '',
            queryData = this.getVarsValues( name, data, keys )
        
        let upsert = ``,
            upsertVals = [ ]
            
        if( opts.upsert ) {
            const upsertKeys = Object.keys( opts.upsert ),
                whereClause = `WHERE ${this.columnToVar( upsertKeys, { alias: name, baseIndex: queryData.vals.length + 1, join: ' AND ' } )}`

            upsert = `ON CONFLICT ( ${upsertKeys.map( key => `"${key}"` ).join(', ')} ) DO UPDATE SET ( ${columns}${nullColumnsStr} ) = ( ${queryData.vars.join(', ')}${nullVals} ) ${whereClause} `
            upsertVals = upsertKeys.map( key => opts.upsert[ key ] )
        }

        return this._factory().query( `INSERT INTO ${name} ( ${columns} ) VALUES ( ${ queryData.vars.join(', ') } ) ${upsert} RETURNING ${this._getSimpleSelect(name)}`, queryData.vals.concat( upsertVals ) )
    }

    select( name, where = { }, opts = { } ) {
        const keys = Object.keys( where ),
            whereClause = keys.length ? `WHERE ${this.columnToVar( keys, { join: ' AND ' } )}` : ``

        return this._factory( opts ).query( `SELECT * FROM ${name} ${whereClause}`, keys.map( key => where[key] ) )
    }

    update( name, patch, where ) {
        const patchKeys = Object.keys( patch ),
            whereKeys = Object.keys( where ),
            allKeys = patchKeys.concat( whereKeys )

        this.validateKeys( name, allKeys )

        return this._factory().query(
            `UPDATE ${name} SET ${ this.columnToVar( patchKeys ) } WHERE ${ this.columnToVar( whereKeys, { baseIndex: patchKeys.length + 1 } ) }`,
            allKeys.map( ( key, i ) => i < patchKeys.length ? patch[ key ] : where[ key ] )
        )
    }

    validateKeys( table, columns ) {
        columns.forEach( column => {
            if( !this.tables[ table ].model.store.name[ column ] ) throw Error(`Invalid Column: ${column}`)
        } )
    }


    getSelectList( table, opts={} ) {
        return typeof table === 'string'
            ? this._getSelect( table, opts.alias ? opts.alias : table )
            : table.map( t => this._getSelect( t, opts.alias ? opts.alias[ t ] : t ) ).join(', ')
    }

    _getSelect( table, alias ) {
        return this.tables[ table ].columns.map( column => `"${alias}"."${column.name}" as "${alias}.${column.name}"` ).join(', ')
    }

    _getSimpleSelect( table ) { return this.tables[ table ].columns.map( column => `"${column.name}"` ).join(', ') }
}

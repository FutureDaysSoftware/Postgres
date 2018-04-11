const selectAllTables = `
SELECT table_name
FROM information_schema.tables
WHERE table_schema='public'
    AND table_type='BASE TABLE'
`;
    
const selectForeignKeys = `
SELECT conrelid::regclass AS tableFrom, conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE contype = 'f' AND n.nspname = 'public'
`;

const selectTableColumns = tableName => {
    return `
        SELECT column_name, data_type, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = '${tableName}'
    `;
}

module.exports = Object.create( {

    Enum: require('../../lib/Enum'),
    
    Model: require('../../lib/Model'),

    enumReference: {
        /*
        tablename: {
            columnName: ENUM
        }
        */
    },

    DataTypeToRange: {
        "bigint": "Integer",
        "boolean": "Boolean",
        "character varying": "Text",
        "date": "Date",
        "integer": "Integer",
        "money": "Float",
        "numeric": "Float",
        "real": "Float",
        "timestamp with time zone": "DateTime",
        "text": "Text"
    },
    
    getColumnDescription( tableName, column ) {
        const isEnum = Boolean( this.enumReference && this.enumReference[ tableName ] && this.enumReference[ tableName ][ column.column_name ] );
              range = isEnum
                ? this.enumReference[ tableName ][ column.column_name ]
                : this.dataTypeToRange[column.data_type]
        return {
            isEnum,
            isNullable: column.is_nullable,
            maximumCharacterLength: column.data_type === "text" ? 1000 : column.character_maximum_length,
            name: column.column_name,
            range
        }
    },

    reflect: async query => {
        const tableData = {}
        const tables =  await query( selectAllTables );

        tables.map( async row => {
            const tableName = row.table_name;
            const tableColumns = await query(selectTableColumns( tableName ));
            tableData[ row.table_name ] = { columns: tableColumns.map( columnRow => this.getColumnDescription(tableName, columnRow) ) }
        });
       
        (await query(selectForeignKeys))
        .forEach( row => {
            const match = /FOREIGN KEY \("?(\w+)"?\) REFERENCES (\w+)\((\w+)\)/.exec( row.pg_get_constraintdef )
            const column = tableData[ row.tablefrom.replace(/"/g,'') ].columns.find( column => column.name === match[1] )
            
            column.fk = {
                table: match[2],
                column: match[3],
            }
        });
        
        const tableNames = Object.keys( tableData );
        tableNames.forEach( tableName => {
            const table = tableData[ tableName ]
            table.model = Object.create( this.Model, { } ).constructor( table.columns, { storeBy: [ 'name' ] } ) 
        } )

        return { resources: tableData, resourceNames: tableNames, tables: tableData }
    }
} ).reflect

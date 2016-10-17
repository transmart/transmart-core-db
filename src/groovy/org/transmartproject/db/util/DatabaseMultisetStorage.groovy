package org.transmartproject.db.util

import org.codehaus.groovy.grails.commons.ApplicationHolder
import org.hibernate.engine.SessionImplementor

import java.sql.Connection

/**
 * Helper class to save multisets of ids in temporary tables in the database
 * that can be ised in 'IN' SQL query conditions, e.g.:
 *   select * from table1 where col1 in (select id from tmp_table where mid=?)
 */
class DatabaseMultisetStorage {

    def databasePortabilityService
    SessionImplementor session
    Integer batchSize
    protected Connection connection
    protected Boolean isPostgres

    enum elementsType {
        INTEGER,
        STRING
    }

    DatabaseMultisetStorage(SessionImplementor sess, Integer bsize) {
        session = sess
        batchSize = bsize
        connection = session.connection()
        if (!databasePortabilityService) {
            databasePortabilityService = ApplicationHolder.getApplication().getMainContext().getBean('databasePortabilityService')
        }
        isPostgres = databasePortabilityService.databaseType == org.transmartproject.db.support.DatabasePortabilityService.DatabaseType.POSTGRESQL

        def isReadOnly = ensureReadWriteTransaction()
        [[getIntegerDataTableName(), 'bigint'], [getStringDataTableName(), 'text']].each {
            if (isPostgres) {
                def sql = 'create temporary table if not exists ' + it[0] + ' (id ' + it[1] + ', mid int not null) on commit preserve rows'
                def stmt = connection.prepareStatement(sql)
                stmt.execute()
            }
            // NOTICE: this commits transaction on Oracle
            def stmt = connection.prepareStatement('truncate table ' + it[0])
            stmt.execute()
        }
        restoreTransactionState(isReadOnly)
    }

    DatabaseMultisetStorage(SessionImplementor sess) {
        this(sess, 1000)
    }

    protected Boolean ensureReadWriteTransaction() {
        Boolean isReadOnly = connection.isReadOnly()
        if (isReadOnly) {
            connection.rollback()
            connection.setReadOnly(false)
        }
        return isReadOnly
    }

    protected void restoreTransactionState(Boolean isReadOnly) {
        if (!isReadOnly) {
            return
        }
        connection.commit()
        connection.setReadOnly(true)
    }

    /**
     * Table name where integer data is stored
     *
     * @return table name
     */
    static String getIntegerDataTableName() {
        return 'session_multisets_of_integers'
    }

    /**
     * Table name where string data is stored
     *
     * @return table name
     */
    static String getStringDataTableName() {
        return 'session_multisets_of_strings'
    }

    /**
     * Store a collection of Integers into temporary table
     *
     * @param data collection of integer values
     * @param session hibernate session
     *
     * @return stored multiset id
     */
    Integer saveIntegerData(Iterable<Long> data) {
        return saveData(data, elementsType.INTEGER)
    }

    /**
     * Store a collection of Strings into temporary table
     *
     * @param data collection of string values
     * @param session hibernate session
     *
     * @return stored multiset id
     */
    Integer saveStringData(Iterable<String> data) {
        return saveData(data, elementsType.STRING)
    }

    protected Integer saveData(data, dataType) {
        if (!data.iterator().hasNext()) {
            return 0
        }

        def multisetId
        def isReadOnly = ensureReadWriteTransaction()
        try {
            def isStringData = dataType == elementsType.STRING
            def tableName = isStringData ? getStringDataTableName() : getIntegerDataTableName()

            def stmt = connection.prepareStatement('select coalesce(max(mid), 0)+1 from ' + tableName)
            def res = stmt.executeQuery()
            res.next()
            multisetId = res.getInt(1)

            def counter = 0
            stmt = connection.prepareStatement('insert into ' + tableName + '(id,mid) values(?,?)')
            data.each {
                if (isStringData) {
                    stmt.setString(1, it)
                } else {
                    stmt.setLong(1, it)
                }
                stmt.setInt(2, multisetId)
                stmt.addBatch()
                counter++
                if (counter >= batchSize) {
                    stmt.executeBatch()
                    counter = 0
                }
            }
            if (counter > 0) {
                stmt.executeBatch()
            }

            // For Oracle use dynamic_sampling(N) hint in queries
            if (isPostgres) {
                stmt = connection.prepareStatement('analyze ' + tableName)
                stmt.execute()
            }
        } finally {
            restoreTransactionState(isReadOnly)
        }

        return multisetId
    }
}


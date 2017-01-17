@Grapes(
        @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.1')
)

import groovy.json.JsonBuilder
import groovyx.net.http.RESTClient

class MoveIndex {
    def sourceHost = "http://localhost:9200"
    def sourceIndexName = 'backup_entities_faultlog_13'
    def destinationHost = "http://localhost:9200"
    def destinationIndexName = 'entities_faultlog_13'
    def docType = 'faultlog'
    def pageSize = 500
    def checkIfExists = true

    def currentScrollId = ""
    def cachedSourceClient = new RESTClient("${sourceHost}".toString())
    def cachedDestinationClient = new RESTClient("${destinationHost}".toString())

    int getNumberOfDocuments() {
        def docCount = 0
        def source = cachedSourceClient
        try {
            def result = source.get path: "/${sourceIndexName}/_search", query: [size: 0]
            docCount = new Integer(result.responseData.hits.total)
        } catch(anything) { println "could not get the document count, returing ${docCount}"}
        docCount
    }


    int calcRequests(int documentCount) {
        int calls = (documentCount / pageSize).toInteger()
        if(documentCount % pageSize > 0) { calls++ }
        calls
    }

    void deleteScrollId(String scrollId) {
        if(!scrollId.isEmpty()) {
            def scroll = cachedSourceClient
            try {
                scroll.delete path: "/_search/scroll/${scrollId}"
            } catch (anything) {
                println "debug: caught ${anything.class.simpleName} while deleting scroll: ${anything.message}"
            }
        }
    }

    boolean destinationDocExists(String id) {
        boolean exists = false
        if(checkIfExists) {
            def check = cachedDestinationClient
            try {
                def result = check.head path: "/${destinationIndexName}/${docType}/${id}"
                if(result.status == 200) { exists = true }
            } catch(anything) {}
        }
        exists
    }

    Map getValidatedResults(data) {
        def results = [:]
        data.each { datum ->
            def source = datum._source
            def key = datum._id ?: "${docType}.${UUID.randomUUID().toString()}.${System.currentTimeMillis().toString()}"
            if (!source.isEmpty() && datum._type == docType && !destinationDocExists(key)) {
                ['afterTreatmentId','engineId'].each {
                    if((source as Map).containsKey(it)) {  }
                }
//            (source as Map).remove('afterTreatmentId')
//            (source as Map).remove('engineId')
                results[key] = source
            }
        }
        results
    }

    Map generateRequestBody() {
        def requestBody = [:]
        if(currentScrollId.isEmpty()) {
            requestBody.put('size', pageSize)
            requestBody.put('sort', ['_doc'])
        } else {
            requestBody.put('scroll_id', currentScrollId)
            requestBody.put('scroll', '2m')
        }
        requestBody
    }

    String generateRequestPath() {
        if(currentScrollId.isEmpty()) {
            "/${sourceIndexName}/_search".toString()
        } else {
            "/_search/scroll"
        }
    }

    Map generateQueryString() {
        if(currentScrollId.isEmpty()) {
            [scroll: '2m']
        } else {
            null
        }
    }

    String convertDataToBulkIndexPayload(Map data) {
        String payload = ""
        data.each { id, source ->
            String jsonData = new groovy.json.JsonBuilder(source).toString()
            payload += "{\"index\": { \"_index\" : \"${destinationIndexName}\", \"_type\" : \"${docType}\", \"_id\" : \"${id}\" }\n${jsonData}\n"
        }
        payload + "\n"
    }

    void writeSuccessWithErrorsData(post, HashMap dataWithIds) {
        String prefix = 'successWithErrors_'
        def unique = System.currentTimeMillis().toString()
        println "\t\twriting out index results and source data to: ${prefix}${unique}.*"
        def successWithErrors = new File(prefix + unique + '.results')
        post.responseData.items.each { resultItem ->
            def json = new JsonBuilder(resultItem).toString() + "\n"
            successWithErrors.append(json)
        }
        new File(prefix + unique + '.data').append(convertDataToBulkIndexPayload(dataWithIds).bytes)
    }

    HashMap getPagedResult(int page) {
        def hits = [:]
        def search = cachedSourceClient
        try {
            def result = search.post(
                path: generateRequestPath(),
                body: generateRequestBody(),
                query: generateQueryString(),
                requestContentType: groovyx.net.http.ContentType.JSON
            )
            currentScrollId = result.responseData._scroll_id
            hits = getValidatedResults(result.responseData.hits.hits)
        } catch(anything) {
            println "\tcaught ${anything.class.simpleName} getting page ${page}: ${anything.message}."
        }
        hits
    }

    void handleBulkIndexException(Map data) {
        try {
            if(data.size()) {
                String unique = System.currentTimeMillis().toString()
                new File('recovery_' + unique + '.dat').append(convertDataToBulkIndexPayload(data).bytes)
                println "\t\t\t wrote offending data to recovery_${unique}.dat"
            }
        } catch(anything) {}
    }

    void bulkIndexData(Map dataWithIds) {
        def bulk = cachedDestinationClient
        try {
            println "\tposting bulk data"
            def post = bulk.post path: '/_bulk', body: convertDataToBulkIndexPayload(dataWithIds).bytes, requestContentType: groovyx.net.http.ContentType.BINARY
            println "\tsuccess: items=${post.responseData.items.size()}, errors=${post.responseData.errors}, time=${post.responseData.took ?: 0}ms"
            if(post.responseData.errors.toString().toLowerCase() != "false") {
                writeSuccessWithErrorsData(post, dataWithIds)
            }
        } catch (anything) {
            println "\t...error: caught ${anything.class.simpleName}: ${anything.message}"
            handleBulkIndexException(dataWithIds)
        }
    }

    void ferryData(int pagedRequests) {
        for (int pageCounter = 0; pageCounter < pagedRequests; pageCounter++) {
            def results = getPagedResult(pageCounter)
            println "${(pageCounter+1).toString().padLeft(6)}: retreived ${results.size()} documents"
            if(results.size() > 0) { bulkIndexData(results) }
        }
        deleteScrollId(currentScrollId)
    }

    void start() {
        println "Getting intial document count."
        def docs = getNumberOfDocuments()
        if(docs > 0) {
            int numberOfPagedRequests = calcRequests(docs)
            println "Found ${docs} documents. This will result in ${numberOfPagedRequests} calls."
            ferryData(numberOfPagedRequests)
        } else {
            println "Nothing found."
        }
        println "done."
    }
}

new MoveIndex().start()
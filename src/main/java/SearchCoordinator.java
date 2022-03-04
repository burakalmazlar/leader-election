import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SearchCoordinator implements OnRequestCallback {
    private static final String ENDPOINT = "/search";
    private static final String BOOKS_DIRECTORY = "./resources/books/";
    private final ServiceRegistry workersServiceRegistry;
    private final WebClient client;
    private final List<String> documents;

    public SearchCoordinator(ServiceRegistry workersServiceRegistry, WebClient client) {
        this.workersServiceRegistry = workersServiceRegistry;
        this.client = client;
        this.documents = readDocumentsList();
    }

    public byte[] handleRequest(byte[] requestPayload) {
        try {
            String request = new String(requestPayload);
            String response = createResponse(request);

            return response.getBytes();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private String createResponse(String searchRequest) throws InterruptedException, KeeperException {

        System.out.println("Received search query: " + searchRequest);

        List<String> searchTerms = TFIDF.getWordsFromLine(searchRequest);

        List<String> workers = workersServiceRegistry.getAllServiceAddresses();

        if (workers.isEmpty()) {
            System.out.println("No search workers currently available");
            return "No search workers currently available";
        }

        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<String> sortedDocuments = aggregateResults(results, searchTerms);

        return sortedDocuments.stream().collect(Collectors.joining("\n"));
    }

    private List<String> aggregateResults(List<Result> results, List<String> terms) {
        Map<String, DocumentData> allDocumentsResults = new HashMap<>();

        for (Result result : results) {
            allDocumentsResults.putAll(result.getDocumentToDocumentData());
        }

        System.out.println("Calculating score for all the documents");
        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentsScores(terms, allDocumentsResults);

        return sortDocumentsByScore(scoreToDocuments);
    }

    private List<String> sortDocumentsByScore(Map<Double, List<String>> scoreToDocuments) {
        List<String> sortedDocumentsStatsList = new ArrayList<>();

        for (Map.Entry<Double, List<String>> docScorePair : scoreToDocuments.entrySet()) {
            double score = docScorePair.getKey();

            for (String document : docScorePair.getValue()) {
                File documentPath = new File(document);

                String documentStats = score  + " " + documentPath.getName() + " " + documentPath.length();

                sortedDocumentsStatsList.add(documentStats);
            }
        }

        return sortedDocumentsStatsList;
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks) {
        CompletableFuture<Result>[] futures = new CompletableFuture[workers.size()];
        for (int i = 0; i < workers.size(); i++) {
            String worker = workers.get(i);
            Task task = tasks.get(i);
            byte[] payload = SerializationUtils.serialize(task);

            futures[i] = client.sendTask(worker, payload);
        }

        List<Result> results = Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());

        System.out.println(String.format("Received %d/%d results", results.size(), tasks.size()));
        return results;
    }

    public List<Task> createTasks(int numberOfWorkers, List<String> searchTerms) {
        List<List<String>> workersDocuments = splitDocumentList(numberOfWorkers, documents);

        List<Task> tasks = new ArrayList<>();

        for (List<String> documentsForWorker : workersDocuments) {
            Task task = new Task(searchTerms, documentsForWorker);
            tasks.add(task);
        }

        return tasks;
    }

    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents) {
        int numberOfDocumentsPerWorker = (documents.size() + numberOfWorkers - 1) / numberOfWorkers;

        List<List<String>> workersDocuments = new ArrayList<>();

        for (int i = 0; i < numberOfWorkers; i++) {
            int firstDocumentIndex = i * numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, documents.size());

            if (firstDocumentIndex >= lastDocumentIndexExclusive) {
                break;
            }
            List<String> currentWorkerDocuments = new ArrayList<>(documents.subList(firstDocumentIndex, lastDocumentIndexExclusive));

            workersDocuments.add(currentWorkerDocuments);
        }
        return workersDocuments;
    }

    private static List<String> readDocumentsList() {
        File documentsDirectory = new File(BOOKS_DIRECTORY);
        return Arrays.asList(documentsDirectory.list())
                .stream()
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }
}
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {

    private WebClient webClient;

    public Aggregator(){
        this.webClient = new WebClient();
    }

    public List<Result> sendTaskToWorkers(List<String> workerAddresses, List<String> tasks) {
        CompletableFuture<Result> [] futures = new CompletableFuture[workerAddresses.size()];

        for (int i = 0; i < workerAddresses.size(); i++) {
            String workerAddress = workerAddresses.get(i);
            String task = tasks.get(i);

            byte [] requestPayload = task.getBytes();

            futures[i] = webClient.sendTask(workerAddress, requestPayload);

        }

        List<Result> results = Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());

        return results;
    }
}

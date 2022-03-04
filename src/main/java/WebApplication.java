import java.util.Arrays;
import java.util.List;

public class WebApplication {

    private static final String WORKER_ADDRES_1 = "http://localhost:8081/task";
    private static final String WORKER_ADDRES_2 = "http://localhost:8082/task";

    public static void main(String[] args) {
        Aggregator aggregator = new Aggregator();

        String task1 = "2,3";
        String task2 = "3453545786456456,4563454564565234,23454654764576,234567547657865743,324564357563474356";

        List<Result> results = aggregator.sendTaskToWorkers(Arrays.asList(WORKER_ADDRES_1, WORKER_ADDRES_2), Arrays.asList(task1, task2));

        results.stream().forEach(System.out::println);
    }
}

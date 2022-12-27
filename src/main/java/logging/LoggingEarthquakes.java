package logging;

import io.restassured.RestAssured;
import io.restassured.response.Response;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;

class LoggingEarthquakes {
    public static void main(String[] args) throws IOException, InterruptedException {

        ServerSocket echoSocket = new ServerSocket(8989);
        Socket socket = echoSocket.accept();


        List<HashMap<String, String>> map = getEarthQuark().jsonPath().get();
        List<String> records = new ArrayList<>();

        map.forEach(item ->{
            String record = item.get("tarih").concat(",")
                    .concat(item.get("saat").concat(","))
                    .concat(item.get("enlem").concat(","))
                    .concat(item.get("derinlik").concat(","))
                    .concat(item.get("buyukluk").concat(","))
                    .concat(item.get("derinlik").concat(","))
                    .concat(item.get("yer").concat(","))
                    .concat(item.get("sehir"));
            records.add(record);
        });

        AtomicInteger border = new AtomicInteger(new Random().nextInt(10) + 1);
        AtomicInteger oldBorder = new AtomicInteger(0);

        System.out.println(records.size());

        do {
            TimeUnit.SECONDS.sleep(2);
            records.subList(oldBorder.get(), border.get()).forEach(record -> {
                try {
                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);

                    out.println(record);
                    System.out.println(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println(oldBorder.get() + " - " + border.get());
            oldBorder.set(border.get());
            border.set(new Random().nextInt(10) + 1 + oldBorder.get());
        } while (border.get() <= records.size());
    }

    protected static Response getEarthQuark(){
        RestAssured.baseURI = "http://localhost";
        RestAssured.basePath = "/api";
        RestAssured.port = 3000;

        return given()
                .when()
                .get()
                .then()
                .extract()
                .response();
    }
}


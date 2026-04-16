import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
        ServerSocket serverSocket = null;
        int port = 6379;

        List<Socket> clients = new ArrayList<>();

        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          // Wait for connection from client.




            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                         PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {

                        String message;
                        while ((message = bufferedReader.readLine()) != null) {
                            if (message.startsWith("*")) {
                                int length = Integer.parseInt(message.substring(1));
                                if (length > 0) {
                                    List<String> aa = new ArrayList<>();
                                    for (int i = 0; i < length; i++) {
                                        int l = Integer.parseInt(bufferedReader.readLine().substring(1));
                                        String m = bufferedReader.readLine();
                                        aa.add(m);
                                    }

                                    for (int i = 0; i < aa.size(); i++) {
                                        if (aa.get(i).equals("PING")) {
                                            printWriter.print("+PONG" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("ECHO")) {
                                            printWriter.print("$" + aa.get(i + 1).length() + "\r\n" + aa.get(i + 1) + "\r\n");
                                            printWriter.flush();
                                        }
                                    }
                                } else {
                                    printWriter.flush();
                                }

                            } else {
                                printWriter.flush();
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Exception: " + e.getMessage());
                    } finally {
                    }


                }).start();
            }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
        }
  }
}

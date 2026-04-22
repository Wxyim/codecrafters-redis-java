import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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

          Map<String, String> map = new ConcurrentHashMap<>();

          Map<String, Date> mapTime = new ConcurrentHashMap<>();

          List<String> list = Collections.synchronizedList(new ArrayList<>());

          Map<String, CopyOnWriteArrayList<String>> mapList = new ConcurrentHashMap<>();

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
                                        if (l == -1) {
                                            aa.add(null);
                                            continue;
                                        }
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
                                        } else if (aa.get(i).equals("SET")) {
                                            if (i + 3 < aa.size()
                                                    && (aa.get(i + 3).equalsIgnoreCase("px")
                                                    || aa.get(i + 3).equalsIgnoreCase("ex"))) {
                                                Date date = aa.get(i + 3).equalsIgnoreCase("px")
                                                        ? new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)))
                                                        : new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)) * 1000);
                                                mapTime.put(aa.get(i + 1), date);
                                            }
                                            map.put(aa.get(i + 1), aa.get(i + 2));
                                            printWriter.print("+OK" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("GET")) {
                                            if (map.containsKey(aa.get(i + 1)) && !mapTime.containsKey(aa.get(i + 1))) {
                                                printWriter.print("$" + map.get(aa.get(i + 1)).length() + "\r\n" + map.get(aa.get(i + 1)) + "\r\n");
                                                printWriter.flush();
                                            } else if (map.containsKey(aa.get(i + 1)) && mapTime.containsKey(aa.get(i + 1))) {
                                                if (mapTime.get(aa.get(i + 1)).before(new Date())) {
                                                    printWriter.print("$-1\r\n");
                                                    printWriter.flush();
                                                    map.remove(aa.get(i + 1));
                                                    mapTime.remove(aa.get(i + 1));
                                                } else {
                                                    printWriter.print("$" + map.get(aa.get(i + 1)).length() + "\r\n" + map.get(aa.get(i + 1)) + "\r\n");
                                                    printWriter.flush();
                                                }
                                            } else {
                                                printWriter.print("$-1\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("RPUSH")) {
                                            CopyOnWriteArrayList<String> tmpList = mapList.getOrDefault(aa.get(i + 1), null);
                                            if (tmpList == null) {
                                                tmpList = new CopyOnWriteArrayList<>();
                                                for (int j = i + 2; j < length; j++) {
                                                    tmpList.add(aa.get(j));
                                                }
                                                mapList.put(aa.get(i + 1), tmpList);
                                                printWriter.print(":" + tmpList.size() + "\r\n");
                                                printWriter.flush();
                                            } else {
                                                for (int j = i + 2; j < length; j++) {
                                                    tmpList.add(aa.get(j));
                                                }
                                                mapList.put(aa.get(i + 1), tmpList);
                                                printWriter.print(":" + tmpList.size() + "\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("LRANGE")) {
                                            CopyOnWriteArrayList<String> tmpList = mapList.getOrDefault(aa.get(i + 1), null);
                                            if (tmpList == null) {
                                                printWriter.print("*0\r\n");
                                                printWriter.flush();
                                            } else {
                                                List<String> tmpList2 = tmpList.subList(Integer.parseInt(aa.get(i + 2)), Integer.parseInt(aa.get(i + 3)) + 1);
                                                printWriter.print("*" + tmpList2.size() + "\r\n");
                                                for (int j = 0; j < tmpList2.size(); j++) {
                                                    printWriter.print("$" + tmpList2.get(j).length() + "\r\n");
                                                    printWriter.print(tmpList2.get(j) + "\r\n");
                                                }
                                                printWriter.flush();
                                            }
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

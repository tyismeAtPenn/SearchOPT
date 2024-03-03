package cis5550.jobs;
import static cis5550.webserver.Server.*;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Coordinator extends cis5550.generic.Coordinator{

        class HighlightGenerator{

            public static String generateHighlight(String queryLine, String page){

                ArrayList<String> query = Query.parseQueryWord(queryLine);
                List<Integer> queryPos = findWordPositions(query,page);
                int centralIndex = findCentralIndexWithRange(queryPos, 100);
                if (centralIndex == -1) {
                    return page.substring(0, Math.min(100, page.length()) );
                }else{
                    return page.substring(centralIndex, Math.min(centralIndex + 100, page.length()));
                }
            }

            private static ArrayList<Integer> findWordPositions(List<String> queryWords, String longString) {
                ArrayList<Integer> positions = new ArrayList<>();

                for (String word : queryWords) {
                    int wordIndex = longString.indexOf(word);
                    while (wordIndex != -1) {
                        positions.add(wordIndex);
                        wordIndex = longString.indexOf(word, wordIndex + 1);
                    }
                }

                return positions;
            }
            private static int findCentralIndexWithRange(List<Integer> positions, int range) {
                if (positions.isEmpty()) {
                    return -1;
                }

                // Calculate the mean of positions
                int sum = positions.stream().mapToInt(Integer::intValue).sum();
                int mean = sum / positions.size();

                // Find the closest position to the mean within [pos, pos + 100)
                int closestIndex = positions.stream()
                        .filter(pos -> pos >= mean && pos < mean + range)
                        .min((pos1, pos2) -> Integer.compare(Math.abs(pos1 - mean), Math.abs(pos2 - mean)))
                        .orElse(-1);

                return closestIndex;
            }

        }
        private static Set<String> stopwords = new HashSet<>();
        public static void main(String[] args) throws Exception {
            port(Integer.parseInt(args[0]));
            String kvsAddress;
            if (args.length > 1) {
                kvsAddress = args[1];
            } else {
                kvsAddress = "localhost:8080";
            }
            registerRoutes();

            get("/query", (req, res) -> {
                String query = req.queryParams("query");
                System.out.println("query:" + req.queryParams("query") );
                String result = Query.QuerySearch(query, kvsAddress);
                return result;
            });

            get("/process", (req, res) -> {
                String hashurl = req.queryParams("url");
                String queryLine = req.queryParams("query");
                if (hashurl.equals("NEEDSPELLCHECK")){
                    Map<String, String> spellCheckPara= new HashMap<>();
                    spellCheckPara.put("url", "localhost:8000");
                    spellCheckPara.put("title", "Do you mean ....");
                    spellCheckPara.put("description", Query.MyChecker.correctSentense(queryLine));
                    return MapToJsonArray(spellCheckPara);
                }
                System.out.println("find url:" + hashurl + " with query:" + queryLine);

                KVSClient kvs = new KVSClient(kvsAddress);

                try {
                    // TODO
                    Row resultRow = kvs.getRow("pt-crawl", hashurl);
                    // TODO
                    String page = resultRow.get("page");
                    System.out.println("page read ok:" + page.length());
                    // parse the title description url
                    Map<String, String> result = new HashMap<>();
                    Document doc = Jsoup.parse(page);
                    Element head = doc.head().selectFirst("meta[name$=description]");
                    if (head != null && !head.attr("content").equals("")){
                        result.put("description",replaceDot(
                                head.attr("content"))
                        );
                    }else{
                        result.put("description",
                                replaceDot(
                                        HighlightGenerator.generateHighlight(queryLine,doc.body().text()))
                        );
                    }
                    System.out.println("description ok");
                    result.put("title", replaceDot(doc.title()));
                    System.out.println("title ok:" + replaceDot(doc.title()));
                    result.put("url", resultRow.get("url"));
                    System.out.println("url ok:" + resultRow.get("url"));
//                    return StringToJsonArray(result);
                    return MapToJsonArray(result);
                } catch (Exception e) {
                    System.out.println("error reading ");
                    System.out.println(e);
                }
                return NullJson();
            });

            get("/", (req, res) -> {
                System.out.println(System.getProperty("user.dir"));
                byte[] encoded = Files.readAllBytes(Paths.get("./frontend/Sample.html"));
                return new String(encoded);
            });
            get("/Search.js", (req, res) -> {
                byte[] encoded = Files.readAllBytes(Paths.get("./frontend/Search.js"));
                return new String(encoded);
            });
        }
    private static String MapToJsonArray(Map<String,String> map){

        JSONObject jsonEntry = new JSONObject();
        for (Map.Entry<String,String> entry : map.entrySet()){
            jsonEntry.put(entry.getKey(),entry.getValue() );
        }
        System.out.println(jsonEntry.toString());
        return jsonEntry.toString();

    }
    private static String replaceDot(String line){
            String finalString = line.replace(",", ".");
            finalString = finalString.replace("{", "(").replace("}", ")");
            finalString = finalString.replace("[","(").replace("]",")");
            return finalString;
    }
    private  static String StringToJsonArray(Map<String, String> data) {
        StringBuilder jsonBuilder = new StringBuilder("{");
        for (String key : data.keySet()) {
            jsonBuilder.append("'" + key + "':'" + data.get(key) + "',");
        }
        if (!data.isEmpty()) {
            jsonBuilder.setLength(jsonBuilder.length() - 1);
        }
        jsonBuilder.append("}");
        System.out.println(jsonBuilder.toString());
        return jsonBuilder.toString();
    }

    private static String NullJson(){
            HashMap<String, String> nullMap = new HashMap<>();
            nullMap.put("description", "null");
            nullMap.put("title", "null");
            nullMap.put("url", "null");
            return MapToJsonArray(nullMap);
//            return StringToJsonArray(nullMap);
    }




}

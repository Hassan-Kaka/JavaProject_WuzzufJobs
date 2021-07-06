package net.codejava;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class Skills {

    @RequestMapping("/skills")
    public String countJopInCompanies(Model model) throws IOException {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        wuzzufDF.createOrReplaceTempView("TRAINING_DATA_5");

        List<String[]> skillListArray = new ArrayList<>();
        wuzzufDF.collectAsList().stream().forEach(row -> skillListArray.add(row.get(7).toString().split(",")));
        List<String> skillList = new ArrayList<>();
        for (String[] array : skillListArray) {
            for (String item : array) {
                skillList.add(item);
            }
        }
        Set<String> set = new HashSet<>(skillList);

        Map<String, Integer> skillOccurencesMap = new HashMap<>();

        set.stream().forEach(item -> skillOccurencesMap.put(item, Collections.frequency(skillList, item)));

        Map<String, Integer> result = skillOccurencesMap.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));



        model.addAttribute("message", result);

        return "skills";
    }
}
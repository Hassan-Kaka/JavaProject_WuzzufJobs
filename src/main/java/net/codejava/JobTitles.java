package net.codejava;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;
import java.util.stream.Collectors;

@Controller
public class JobTitles {

    @RequestMapping("/jobtitles")
    public String countJopInCompanies(Model model) {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        final Dataset<Row> countTitlesDF = wuzzufDF.groupBy("Title").count();
        Map<String, Integer> map = new HashMap<>();
        countTitlesDF.collectAsList().stream().forEach(row -> map.put(row.get(0).toString(), Integer.parseInt(row.get(1).toString())));
        Map<String, Integer> result = map.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        model.addAttribute("message", result);

        return "jobtitles";
    }


    }

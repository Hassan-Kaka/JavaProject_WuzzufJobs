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
import java.util.stream.Collectors;

@Controller
public class CompaniesJop {

    @RequestMapping("/companiesjob")
    public String countJopInCompanies(Model model) throws IOException {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        final Dataset<Row> countJobsDF = wuzzufDF.groupBy("Company").count();

        Map<String, Integer> map = new HashMap<>();
        countJobsDF.collectAsList().stream().forEach(row -> map.put(row.get(0).toString(), Integer.parseInt(row.get(1).toString())));
        Map<String, Integer> result = map.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        System.out.println(result.keySet());
        System.out.println(result.values());


        model.addAttribute("message", result);


        return "companiesjob";
    }
}

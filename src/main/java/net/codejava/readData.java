package net.codejava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;

@Controller
public class readData {
    @RequestMapping("/readSomeOfData")
    public String someData(Model model) {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        List<String[]> top20 = new ArrayList<>();
        wuzzufDF.collectAsList().subList(0,21).stream().forEach(row -> top20.add(row.toString().split(",")));

        model.addAttribute("message", top20);

        return "data";
    }

    @RequestMapping("/displayStructure")
    public String structure(Model model) {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        List<String> struct = new ArrayList<>();
        Arrays.stream(wuzzufDF.schema().fields()).forEach(row -> struct.add(row.toString()));

        model.addAttribute("message", struct);

        return "structure";
    }

    @RequestMapping("/displaySummary")
    public String summary(Model model) {

        final Dataset<Row> wuzzufDF = new WuzzufJopsDAO().getDataset();

        List<String> summary = new ArrayList<>();
        wuzzufDF.describe().collectAsList().forEach(row -> summary.add(row.toString()));

        model.addAttribute("message", summary);

        return "summary";
    }

}

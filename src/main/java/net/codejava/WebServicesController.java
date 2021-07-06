package net.codejava;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebServicesController {
    @GetMapping("/rest")
    public String sayRest(){
        return "i love u too 123";
    }
}

package com.sonodavide.opencoesioneanalysis.controller;


import com.sonodavide.opencoesioneanalysis.service.OpencoesioneDataUpdaterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/opencoesione")
public class DataUpdaterController {
    private final OpencoesioneDataUpdaterService opencoesioneDataUpdaterService;
    @Autowired
    public DataUpdaterController(OpencoesioneDataUpdaterService opencoesioneDataUpdaterService) {
        this.opencoesioneDataUpdaterService = opencoesioneDataUpdaterService;
    }
    @GetMapping("/update")
    public ResponseEntity<?> opencoesione(@RequestParam(name= "overrideUpdate", defaultValue = "false")boolean overrideUpdate) {
        try{
            opencoesioneDataUpdaterService.updateData(overrideUpdate);
            return ResponseEntity.ok().build();
        }catch(Exception e){
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @GetMapping("test")
    public ResponseEntity<?> test(){
        return ResponseEntity.ok().build();
    }
}

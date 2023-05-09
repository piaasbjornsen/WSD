package wdm.stock.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import wdm.stock.exception.StockLimitReachedException;
import wdm.stock.exception.StockNotFoundException;
import wdm.stock.model.Stock;
import wdm.stock.repository.StockRepository;

import java.util.Collections;
import java.util.Map;

@RestController
public class StockController {

    private final StockRepository repository;

    public StockController(StockRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/find/{item_id}")
    Stock findStock(@PathVariable String item_id){
        return repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
    }

    @PostMapping("/item/create/{price}")
    Map<String,String> createItem(@PathVariable float price){
        Stock tmp = new Stock(0, price);
        repository.save(tmp);
        return Collections.singletonMap("item_id", tmp.idGet());
    }

    @PostMapping("/subtract/{item_id}/{amount}")
    @ResponseStatus(value = HttpStatus.OK)
    void subtractStock(@PathVariable String item_id, @PathVariable int amount){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        if (tmp.getStock() < amount) {
            throw new StockLimitReachedException(tmp.getStock(), amount);
        }
        else {
            tmp.setStock(tmp.getStock() - amount);
            repository.save(tmp);
        }
    }

    @PostMapping("/add/{item_id}/{amount}")
    @ResponseStatus(value = HttpStatus.OK)
    void addStock(@PathVariable String item_id, @PathVariable int amount){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        tmp.setStock(tmp.getStock()+amount);
        repository.save(tmp);
    }
    
}
package com.example.ElasticSearchDemo.controller;

import com.example.ElasticSearchDemo.entity.BookEntity;
import com.example.ElasticSearchDemo.service.BookService;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class SearchController {

    @Autowired private BookService bookService;

    /**
     * 根据Id查找
     * @param id
     * @return
     */
    @GetMapping("/get/book")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name = "id",defaultValue = "") String id){
        GetResponse response = bookService.getById(id);
        if(!response.isExists()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(response.getSource(),HttpStatus.OK);
    }

    /**
     * 添加
     * @param name 书名
     * @param publishDate 出版日期
     * @param price 价格
     * @param author 作者
     * @return
     */
    @PostMapping("add/book")
    @ResponseBody
    public ResponseEntity add(
            @RequestParam(name="name") String name,
            @RequestParam(name="publishDate") String publishDate,
            @RequestParam(name="price") Double price,
            @RequestParam(name="author") String author
            ){
        IndexResponse response;
        try {
            response = bookService.add(name,publishDate,price,author);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity(response,HttpStatus.OK);
    }

    /**
     * 删除
     * @param id
     * @return
     */
    @DeleteMapping("remove/book")
    public ResponseEntity remove(@RequestParam(name="id") String id){
        DeleteResponse response = bookService.remove(id);
        return new ResponseEntity(response.getResult().toString(),HttpStatus.OK);
    }

    /**
     * 修改ES数据
     * @param id
     * @param name
     * @param publishDate
     * @param price
     * @param author
     * @return
     */
    @PutMapping("modify/book")
    @ResponseBody
    public ResponseEntity modify(@RequestParam(name ="id") String id,
                                 @RequestParam(name="name",required = false) String name,
                                 @RequestParam(name="publishDate",required = false) String publishDate,
                                 @RequestParam(name="price",required = false) Double price,
                                 @RequestParam(name="author",required = false) String author){
        UpdateResponse response;
        try {
            response = bookService.modify(id, name, publishDate,price,author);
        }catch (Exception e){
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity(response.getResult().toString(),HttpStatus.OK);
    }


    /**
     * 查询所有数据
     * @return
     */
    @GetMapping("/get/matchAllQuery")
    @ResponseBody
    public Object matchAllQuery(){
        List<BookEntity> bookEntities = bookService.matchAllQuery();
        return bookEntities;
    }

    /**
     * 分页查询
     * @param pageNum 页数
     * @param pageSize 条数
     * @return
     */
    @GetMapping("/get/getPageQuery")
    @ResponseBody
    public Object getPageQuery(@RequestParam(name="pageNum") Integer pageNum,
                                @RequestParam(name="pageSize") Integer pageSize){
        List<BookEntity> bookEntities = bookService.getPageQuery(pageNum,pageSize);
        return bookEntities;
    }

    /**
     * 查询书籍中包含 Java 关键字，且按发版日期降序排序
     * @param name 书籍名称
     * @return
     */
    @GetMapping("/get/matchQueryAndSort")
    @ResponseBody
    public Object matchQueryAndSort(@RequestParam(name="name") String name){
        List<BookEntity> bookEntities = bookService.matchQueryAndSort(name);
        return bookEntities;
    }


    /**
     * 根据价格聚合查询
     * @return
     */
    @GetMapping("/get/priceAggregations")
    public void priceAggregations(){
        bookService.priceAggregations();
    }


    /**
     * 按照价格以及日期分组查询
     */
    @GetMapping("/get/priceAndPublishDateGroup")
    @ResponseBody
    public String priceAndPublishDateGroup(){
        String jsonData = bookService.priceAndPublishDateGroup();
        return jsonData;
    }
}

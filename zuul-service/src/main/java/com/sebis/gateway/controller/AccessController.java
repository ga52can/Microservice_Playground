package com.sebis.gateway.controller;

import org.springframework.web.bind.annotation.*;

import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;

@RestController
public class AccessController {

    @RequestMapping(value = {"/"}, method = RequestMethod.GET)
    @ResponseBody
    public ModelAndView welcomePage(HttpServletRequest request) {
        ModelAndView model = new ModelAndView();
        model.setViewName("redirect:/business-core-service/businesses/list");
        return model;
    }
    
}

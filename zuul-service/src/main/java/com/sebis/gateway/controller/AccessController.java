package com.sebis.gateway.controller;

import org.springframework.web.bind.annotation.*;

import org.springframework.web.servlet.ModelAndView;

@RestController
public class AccessController {

    @RequestMapping(value = { "/", "/welcome**" }, method = RequestMethod.GET)
    @ResponseBody
    public ModelAndView welcomePage() {
        ModelAndView model = new ModelAndView();
        model.setViewName("home");
        return model;
    }
    
}

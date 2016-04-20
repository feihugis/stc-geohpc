package edu.gmu.stc.website.mvc.view;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Created by Fei Hu on 4/19/16.
 */

@Configuration
@ComponentScan
@Controller
public class MvcConfig extends WebMvcConfigurerAdapter {

  @Override
  public void addViewControllers(ViewControllerRegistry registry) {
    registry.addViewController("/home").setViewName("home");
    registry.addViewController("/").setViewName("index");
  }

  public static void main(String[] args) throws Exception {
    new SpringApplicationBuilder(MvcConfig.class).run(args);
  }
}

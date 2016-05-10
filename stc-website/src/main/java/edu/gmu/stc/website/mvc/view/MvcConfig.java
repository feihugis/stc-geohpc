package edu.gmu.stc.website.mvc.view;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import edu.gmu.stc.website.WebProperties;

/**
 * Created by Fei Hu on 4/19/16.
 */

@Configuration
@ComponentScan
@Controller
public class MvcConfig extends WebMvcConfigurerAdapter {

  @Override
  public void addViewControllers(ViewControllerRegistry registry) {
    registry.addViewController("/index").setViewName("taylordiagram");
    registry.addViewController("/index.html").setViewName("taylordiagram");
    registry.addViewController("/").setViewName("index");
    registry.addViewController("/query").setViewName("query");
    registry.addViewController("/taylordiagram").setViewName("taylordiagram");
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    WebProperties.initilizeProperties();
    String gifLocation = "file:" + WebProperties.GIF_PATH;   //"file:/Applications/apache-tomcat-8.0.14/webapps/gif/"
    String resultLocation = "file:" + WebProperties.RESULT_PATH;
    registry.addResourceHandler("/gif/**").addResourceLocations(gifLocation);// + WebProperties.GIF_PATH);
    registry.addResourceHandler(WebProperties.RESULT_URI+"**").addResourceLocations(resultLocation);
    super.addResourceHandlers(registry);
  }

  public static void main(String[] args) throws Exception {
    new SpringApplicationBuilder(MvcConfig.class).run(args);
  }
}

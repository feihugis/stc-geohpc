package edu.gmu.stc.hadoop.raster.index.hibernate;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;

import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 8/24/16.
 */
public class HibernateUtils {
  private static StandardServiceRegistry registry;
  private static SessionFactory sessionFactory;

  public static SessionFactory createSessionFactoryWithPhysicalNamingStrategy(String hibernateConfigXml, String tableName) {
    registry = new StandardServiceRegistryBuilder().configure(hibernateConfigXml).build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

      //metadataBuilder.applyPhysicalNamingStrategy(siaCollection);
      sessionFactory = metadataBuilder.build().buildSessionFactory();

    } catch (Exception e) {
      StandardServiceRegistryBuilder.destroy(registry);
    }

    return sessionFactory;
  }

  public static SessionFactory createSessionFactory(String hibernateConfigXml) {
    registry = new StandardServiceRegistryBuilder().configure(hibernateConfigXml).build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

      sessionFactory = metadataBuilder.build().buildSessionFactory();

    } catch (Exception e) {
      StandardServiceRegistryBuilder.destroy(registry);
    }

    return sessionFactory;
  }

  public static void main(String[] args) {
    String hbcCfgXML = "mod08_d3_hibernate.cfg.xml";
    SessionFactory sessionFactory = HibernateUtils.createSessionFactory(hbcCfgXML);
    Session session = sessionFactory.openSession();
    DataChunk dataChunk = new DataChunk();
    dataChunk.setFilePos(100L);
    dataChunk.setByteSize(101L);
    dataChunk.setTime(2016);
    dataChunk.setCorner(new int[]{1,2,3});

    Criteria criteria = session.createCriteria(DataChunk.class);
    //criteria.add(Restrictions.gt("time", 1000));
    //List<DataChunk> dataChunkList = criteria.list();
    //System.out.println(criteria.list().size());
    session.beginTransaction();
    session.save(dataChunk);
    session.getTransaction().commit();
    session.close();
  }

}

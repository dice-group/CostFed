package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelException;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.FedXFactory;

public class CostFedRepositoryFactory implements RepositoryFactory {
    final static Logger log = LoggerFactory.getLogger(CostFedRepositoryFactory.class);
    
    static class CostFedRepositoryConfig extends AbstractRepositoryImplConfig {
        public static final String NAMESPACE = "http://www.openrdf.org/config/repository/costfed#";
        public final static IRI FILENAME;
        static {
            ValueFactory factory = SimpleValueFactory.getInstance();
            FILENAME   = factory.createIRI(NAMESPACE, "config"); 
        }
        
        String configFileName;
        
        @Override
        public void validate() throws RepositoryConfigException {
            super.validate();
            if (configFileName == null || !new File(configFileName).exists()) {
                throw new SailConfigException("No valid config file name specified for CostFed repository");
            }
        }

        @Override
        public Resource export(Model model) {
            Resource implNode = super.export(model);
            SimpleValueFactory vf = SimpleValueFactory.getInstance();

            if (configFileName != null) {
                model.add(implNode, FILENAME, vf.createLiteral(configFileName));
            }

            return implNode;
        }

        void setConfigFile(String val) {
            configFileName = val;
        }
        
        String getConfigFile() {
            return configFileName;
        }
        
        @Override
        public void parse(Model model, Resource resource) throws RepositoryConfigException {
            super.parse(model, resource);
            try {
                Optional<Literal> cfg = Models.objectLiteral(model.filter(resource, FILENAME, null));
                setConfigFile(cfg.get().getLabel());
            } catch (ModelException e) {
                throw new RepositoryConfigException(e.getMessage(), e);
            }
        }
    }
    
    @Override
    public String getRepositoryType() {
        return "openrdf:CostFedRepository";

    }

    @Override
    public RepositoryImplConfig getConfig() {
        return new CostFedRepositoryConfig();
    }

    @Override
    public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
        log.info("getting repository");
        try {
            if (config instanceof CostFedRepositoryConfig) {
                CostFedRepositoryConfig cfg = (CostFedRepositoryConfig)config;
                log.info("config: " + cfg.getConfigFile());
                return FedXFactory.initializeFederation(cfg.getConfigFile());
            }
            log.info("no repository");
            return null;
        } catch (Exception e) {
            throw new RepositoryConfigException(e.getMessage(), e);
        }
    }
}

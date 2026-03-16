/*
 * Copyright 2025, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package incsteps.plugin.oci

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.nio.OciFileSystemProvider
import nextflow.file.FileHelper
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

/**
 * The plugin entry point
 */
@CompileStatic
@Slf4j
class OciPlugin extends BasePlugin {

    OciPlugin(PluginWrapper wrapper) {
        super(wrapper)
        pluginClassLoader = wrapper.pluginClassLoader
        showBanner()
    }

    static void showBanner() {
        log.info(" ___                                             _        _ ")
        log.info("|_ _|_ __   ___ _ __ ___ _ __ ___   ___ _ __  | |_ __ _| |")
        log.info(" | || '_ \\ / __| '__/ _ \\ '_ ` _ \\ / _ \\ '_ \\ | __/ _` | |")
        log.info(" | || | | | (__| | |  __/ | | | | |  __/ | | || | || (_| | |")
        log.info("|___|_| |_|\\___|_|  \\___|_| |_| |_|\\___|_| |_||_| \\__|\\__,_|_|")
        log.info("")
        log.info("   ____  _                 ")
        log.info("  / ___|| |_ ___ _ __  ___ ")
        log.info("  \\___ \\| __/ _ \\ '_ \\/ __|")
        log.info("   ___) | ||  __/ |_) \\__ \\")
        log.info("  |____/ \\__\\___| .__/|___/")
        log.info("                |_|        ")
        log.info("")
        log.info("  :: http://nextflow.incsteps.com :: ")
        log.info("")
    }

    private static ClassLoader pluginClassLoader
    static ClassLoader getPluginClassLoader(){
        pluginClassLoader
    }

    @Override
    void start() {
        super.start()
        FileHelper.getOrInstallProvider(OciFileSystemProvider)
    }
}

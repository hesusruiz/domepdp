window.onload = function () {
   //<editor-fold desc="Changeable Configuration Block">

   // the following lines will be replaced by docker/configurator, when it runs in a docker-container
   window.ui = SwaggerUIBundle({
      url: "/oapi/swagger/TMF620-Product_Catalog_Management-v5.0.0.oas.yaml",
      urls: [
         { url: "/oapi/swagger/TMF620-Product_Catalog_Management-v5.0.0.oas.yaml", name: "Product Catalog Management" },
         { url: "/oapi/swagger/TMF632-Party_Management-v5.0.0.oas.yaml", name: "Party Management" },
      ],
      dom_id: "#swagger-ui",
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
      plugins: [SwaggerUIBundle.plugins.DownloadUrl],
      layout: "StandaloneLayout",
   });

   //</editor-fold>
};

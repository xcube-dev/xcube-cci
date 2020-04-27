Attributes {
    lon {
        String standard_name "longitude";
        String long_name "longitude, from -180 (west) to +180 (east) given at gridcell centers";
        String units "degree_east";
        Int32 _ChunkSizes 360;
    }
    lat {
        String standard_name "latitude";
        String long_name "latitude, from -90 (south) to +90 (north) given at gridcell centers";
        String units "degree_north";
        Int32 _ChunkSizes 180;
    }
    layers {
        String standard_name "model_level_number";
        String long_name "model_level_number == layer number, starting at 1";
        String units "1";
        Int32 _ChunkSizes 16;
    }
    air_pressure {
        String standard_name "air_pressure";
        String long_name "air pressure at layer boundaries";
        String NOTE "Replace the first element from this array with the corresponding surface pressure element.";
        String units "hPa";
        Int32 _ChunkSizes 17;
    }
    time {
        String standard_name "time";
        String units "seconds since 1997-01-04";
        String calendar "standard";
        String long_name "time";
        Int32 _ChunkSizes 1;
    }
    surface_pressure {
        String standard_name "surface_air_pressure";
        String long_name "Pressure at the bottom of the atmosphere.";
        String units "hPa";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 180, 360;
    }
    O3_du {
        String standard_name "mole_content_of_ozone_in_atmosphere_layer";
        String long_name "weighted average of the partial ozone columns (DU/layer)";
        String units "446.15E-6 mol m-2";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 8, 180, 360;
    }
    O3e_du {
        String standard_name "mole_content_of_ozone_in_atmosphere_layer standard_error";
        String long_name "error in the weighted average of the partial ozone columns (DU/layer)";
        String units "446.15E-6 mol m-2";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 8, 180, 360;
    }
    O3_du_tot {
        String standard_name "atmosphere_mole_content_of_ozone";
        String long_name "total column: vertically integrated O3_du dataset (DU)";
        String units "446.15E-6 mol m-2";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 180, 360;
    }
    O3e_du_tot {
        String standard_name "atmosphere_mole_content_of_ozone standard_error";
        String long_name "total column error: quadratically added o3e_du (DU)";
        String units "446.15E-6 mol m-2";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 180, 360;
    }
    O3_vmr {
        String standard_name "mole_fraction_of_ozone_in_air";
        String long_name "weighted average of the volume mixing ratio (ppmv)";
        String units "1e-6";
        Float32 _FillValue NaN;
        Int32 _ChunkSizes 1, 9, 90, 180;
    }
    O3e_vmr {
        Float32 _FillValue NaN;
        String standard_name "mole_fraction_of_ozone_in_air standard_error";
        String long_name "error in the weighted average of the volume mixing ratio (ppmv)";
        String units "1e-6";
        Int32 _ChunkSizes 1, 9, 90, 180;
    }
    O3_ndens {
        String standard_name "mole_concentration_of_ozone_in_air";
        String long_name "weighted average of the number density (#molecules/cm3)";
        String units "6.02214E-17 mol m-3";
        Float32 _FillValue 5;
        Int32 _ChunkSizes 1, 9, 90, 180;
    }
    O3e_ndens {
        Float32 _FillValue -32.0;
        String units "6.02214E-17 mol m-3";
        String standard_name "mole_concentration_of_ozone_in_air standard_error";
        String long_name "error in the weighted average of the number density (#molecules/cm3)";
        Int32 _ChunkSizes 1, 9, 90, 180;
    }
    NC_GLOBAL {
        String title "ESA CCI ozone level 3 monthly.";
        String institution "Royal Netherlands Meteorological Institute, KNMI";
        String source "This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 gridded onto a global grid.";
        String history "L2 data gridded to global grid.";
        String references "http://www.esa-ozone-cci.org/";
        String tracking_id "32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775";
        String Conventions "CF-1.6";
        String product_version "fv0002";
        String summary "This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 gridded onto a global grid.";
        String keywords "satellite, observation, atmosphere, ozone";
        String id "32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775";
        String naming_authority "KNMI, http://www.knmi.nl/";
        String comment "These data were produced at KNMI as part of the ESA OZONE CCI project.";
        String date_created "2014-01-08T12:50:21.908";
        String creator_name "J.C.A. van Peet";
        String creator_url "KNMI, http://www.knmi.nl/";
        String creator_email "peet@knmi.nl";
        String project "Climate Change Initiative - European Space Agency";
        Float32 geospatial_lat_min -90.0;
        Float32 geospatial_lat_max 90.0;
        String geospatial_lat_units "degree_north";
        Float32 geospatial_lat_resolution 1.0;
        Float32 geospatial_lon_min -180.0;
        Float32 geospatial_lon_max 180.0;
        String geospatial_lon_units "degree_east";
        Float32 geospatial_lon_resolution 1.0;
        Float32 geospatial_vertical_min 0.01;
        Float32 geospatial_vertical_max 1013.0;
        String time_coverage_start "19970104T102333Z";
        String time_coverage_end "19970131T233849Z";
        String time_coverage_duration "P1M";
        String time_coverage_resolution "P1M";
        String standard_name_vocabulary "NetCDF Climate and Forecast(CF) Metadata Convention version 20, 11 September 2012";
        String license "data use is free and open";
        String platform "merged: ERS-2, ENVISAT, EOS-AURA, METOP-A";
        String sensor "merged: GOME, SCIAMACHY, OMI and GOME-2.";
        String spatial_resolution "see geospatial_lat_resolution and geospatial_lat_resolution";
        String Note "netCDF compression applied.";
    }
}
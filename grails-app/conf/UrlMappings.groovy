class UrlMappings {

	static mappings = {
		"/v1/$controller/$action?/$id?"{
			constraints {
				// apply constraints here
			}
		}

		"/v1/"(view:"/index")
		"500"(view:'/error')
	}
}
